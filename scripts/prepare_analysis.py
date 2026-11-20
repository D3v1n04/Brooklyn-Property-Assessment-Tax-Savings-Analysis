from pathlib import Path
import csv

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_DIR / "data/processed"
OUT_DIR = PROCESSED_DIR / "analysis"
EXCEL_DIR = OUT_DIR / "excel"
REPORT_DIR = PROJECT_DIR / "reports/analysis_preparation"

BOROUGHS = [
    "manhattan", "bronx", "brooklyn", "queens", "staten_island"
]

GROUPS = {
    "Walk-up apartments": ["C1", "C2", "C3", "C4", "C9", "CB"],
    "Elevator apartments": ["D1", "D3", "D5", "D8", "D9", "DB"],
    "Office buildings": ["O1", "O2", "O3", "O4", "O5", "O6"],
    "Retail buildings": ["K1", "K2"],
}

RESIDENTIAL_CLASSES = ["2", "2A", "2B", "2C"]

# A smaller set for Excel; original detail remains in processed files.
EXCEL_COLUMNS = [
    "source_row_id", "parid", "year", "boro", "borough_name",
    "housenum_lo", "housenum_hi", "street_name", "zip_code",
    "cpb_boro", "cpb_dist",
    "property_group", "bldg_class", "fintaxclass",
    "gross_sqft", "land_area", "bld_story", "num_bldgs",
    "yrbuilt", "yrbuilt_flag", "yralt1", "yralt2", "units",
    "finmkttot", "finacttot", "finactextot",
    "fintrntot", "fintrnextot", "fintxbtot", "fintxbextot",
    "fintaxflag", "tenmkttot", "tenacttot",
    "tentxbtot", "tentxbextot",
    "zip_needs_review", "age_needs_review",
    "size_details_need_review", "analysis_status",
]


def main():
    # Confirm the preceding quality-check run finished successfully.
    check_path = PROJECT_DIR / "reports/data_quality/check_summary.csv"
    with check_path.open(encoding="utf-8", newline="") as file:
        checks = list(csv.DictReader(file))

    if not checks or any(row["status"] != "PASS" for row in checks):
        raise RuntimeError("Resolve existing quality-check failures first.")

    # Refuse to overwrite a previous preparation run.
    if OUT_DIR.exists() and any(OUT_DIR.iterdir()):
        raise RuntimeError(
            "Analysis outputs already exist. Archive them before rerunning."
        )

    paths = [
        PROCESSED_DIR / f"{borough}.parquet"
        for borough in BOROUGHS
    ]
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(path)

    lf = pl.scan_parquet([str(path) for path in paths])

    # Explicit building-code and tax-class combinations.
    group_expression = pl.lit(None, dtype=pl.String)

    for group, codes in GROUPS.items():
        tax_condition = (
            pl.col("fintaxclass").is_in(RESIDENTIAL_CLASSES)
            if group in {"Walk-up apartments", "Elevator apartments"}
            else pl.col("fintaxclass") == "4"
        )
        group_expression = (
            pl.when(pl.col("bldg_class").is_in(codes) & tax_condition)
            .then(pl.lit(group))
            .otherwise(group_expression)
        )

    selected = (
        lf.with_columns(group_expression.alias("property_group"))
        .filter(pl.col("property_group").is_not_null())
    )

    # Relevant numeric failures only; unrelated fields do not
    # automatically disqualify a record.
    rules = {
        "missing_property_id": pl.col("missing_property_id"),
        "invalid_year": pl.col("invalid_or_missing_year"),
        "invalid_square_footage": (
            pl.col("gross_sqft").is_null()
            | (pl.col("gross_sqft") <= 0)
        ),
        "invalid_assessed_value": (
            pl.col("finacttot").is_null()
            | (pl.col("finacttot") <= 0)
        ),
        "value_in_other_lot": pl.col("value_reflected_in_other_lot"),
        "building_in_progress": pl.col("building_in_progress"),
    }

    selected = selected.with_columns([
        expression.fill_null(True).alias(f"exclude_{name}")
        for name, expression in rules.items()
    ])

    exclusion_columns = [f"exclude_{name}" for name in rules]

    selected = selected.with_columns([
        (
            ~pl.any_horizontal([
                pl.col(column) for column in exclusion_columns
            ])
        ).alias("basic_eligible"),

        pl.concat_str(
            [
                pl.when(pl.col(f"exclude_{name}"))
                .then(pl.lit(name))
                .otherwise(pl.lit(None, dtype=pl.String))
                for name in rules
            ],
            separator="; ",
            ignore_nulls=True,
        ).alias("exclusion_reasons"),

        (
            pl.col("yrbuilt").is_null()
            | (pl.col("yrbuilt") <= 0)
            | (
                pl.col("yrbuilt")
                > pl.col("year").cast(pl.Int64, strict=False)
            )
        ).fill_null(True).alias("age_needs_review"),

        (
            pl.col("bld_story").is_null()
            | (pl.col("bld_story") <= 0)
            | pl.col("num_bldgs").is_null()
            | (pl.col("num_bldgs") != 1)
        ).fill_null(True).alias("size_details_need_review"),
    ]).with_columns(
        pl.when(pl.col("basic_eligible"))
        .then(pl.lit("pending_peer_checks"))
        .otherwise(pl.lit("excluded_from_initial_comparison"))
        .alias("analysis_status")
    )

    # The selected pool is about 367k records. Collect only
    # the Excel fields, eligibility fields, and audit flags.
    audit_columns = list(dict.fromkeys(
        EXCEL_COLUMNS
        + ["basic_eligible", "exclusion_reasons"]
        + exclusion_columns
    ))

    print("Preparing the four property groups...", flush=True)
    data = selected.select(audit_columns).collect(engine="streaming")

    if data.is_empty():
        raise RuntimeError("No selected properties found.")

    if data["source_row_id"].n_unique() != data.height:
        raise RuntimeError("Repeated API IDs in selected records.")

    eligible = data.filter(pl.col("basic_eligible"))
    excluded = data.filter(~pl.col("basic_eligible"))

    if eligible.height + excluded.height != data.height:
        raise RuntimeError("Selection counts do not reconcile.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    EXCEL_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # All selected records remain available, including exclusions.
    data.write_parquet(OUT_DIR / "selected_properties_audit.parquet")
    eligible.write_parquet(OUT_DIR / "comparison_candidates.parquet")

    summary = (
        data.group_by(["property_group", "year"])
        .agg([
            pl.len().alias("selected_records"),
            pl.col("basic_eligible").sum().alias("basic_eligible_records"),
            (~pl.col("basic_eligible")).sum().alias("excluded_records"),
            pl.col("zip_needs_review").sum().alias("zip_flags"),
            pl.col("age_needs_review").sum().alias("age_flags"),
            pl.col("size_details_need_review").sum().alias("size_flags"),
        ])
        .sort(["property_group", "year"])
    )
    summary.write_csv(REPORT_DIR / "selection_summary.csv")

    reasons = pl.DataFrame([
        {
            "reason": name,
            "records": data[f"exclude_{name}"].sum(),
        }
        for name in rules
    ])
    reasons.write_csv(REPORT_DIR / "exclusion_reason_counts.csv")

    exports = []

    for borough in BOROUGHS:
        # Borough labels use spaces, filenames use underscores.
        label = borough.replace("_", " ").title()
        borough_data = eligible.filter(pl.col("borough_name") == label)

        for year in sorted(borough_data["year"].unique().to_list()):
            partition = borough_data.filter(pl.col("year") == year)

            if partition.height > 1_048_575:
                raise RuntimeError(f"{borough} {year} exceeds Excel's limit.")

            filename = f"{borough}_{year}.csv"
            partition.select(EXCEL_COLUMNS).write_csv(EXCEL_DIR / filename)

            exports.append({
                "file": filename,
                "records": partition.height,
            })
            print(f"Exported {filename}: {partition.height:,} records")

    if exports:
        pl.DataFrame(exports).write_csv(REPORT_DIR / "export_counts.csv")

    if sum(row["records"] for row in exports) != eligible.height:
        raise RuntimeError("Excel export counts do not reconcile.")

    print(f"\nSelected records: {data.height:,}")
    print(f"Basic eligible:   {eligible.height:,}")
    print(f"Excluded:         {excluded.height:,}")
    print(f"Excel CSV files:  {len(exports)}")
    print(f"Reports: {REPORT_DIR}")
    print(
        "\nLocation, age, size, and peer-group coverage still need review. "
        "No assessment benchmarks or tax savings have been calculated."
    )


if __name__ == "__main__":
    main()
