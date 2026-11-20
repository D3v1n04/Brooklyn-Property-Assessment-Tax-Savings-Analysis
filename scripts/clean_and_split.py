import hashlib
import json
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data/raw/assessment_batches"
OUT_DIR = PROJECT_DIR / "data/processed"
EXCEL_DIR = OUT_DIR / "excel"
REPORT_DIR = PROJECT_DIR / "reports/cleaning"

BOROUGHS = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
}

TARGET_CLASSES = ["2", "2A", "2B", "2C", "4"]
MAX_EXCEL_DATA_ROWS = 1_048_575

# These fields remain text: identifiers, codes, year, and dates.
NUMERIC_COLUMNS = [
    "bld_story",
    "num_bldgs",
    "yrbuilt",
    "yralt1",
    "yralt2",
    "units",
    "coop_apts",
    "land_area",
    "gross_sqft",
    "residential_area_gross",
    "office_area_gross",
    "retail_area_gross",
    "hotel_area_gross",
    "factory_area_gross",
    "warehouse_area_gross",
    "finmktland",
    "finmkttot",
    "finacttot",
    "finactextot",
    "fintrntot",
    "fintrnextot",
    "fintxbtot",
    "fintxbextot",
    "tenmkttot",
    "tenacttot",
    "tentxbtot",
    "tentxbextot",
]


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    manifest = json.loads(
        (RAW_DIR / "manifest.json").read_text(encoding="utf-8")
    )

    if manifest.get("verified") is not True:
        raise RuntimeError("Download is not marked as verified.")

    files = []
    offset = 0
    seen_files = set()

    print("Checking raw batches...", flush=True)

    for batch in manifest["batches"]:
        filename = batch["file"]
        path = RAW_DIR / filename

        if filename in seen_files or batch["offset"] != offset:
            raise RuntimeError("Manifest batch sequence is invalid.")

        if not path.is_file() or file_hash(path) != batch["sha256"]:
            raise RuntimeError(f"Missing or changed batch: {path}")

        seen_files.add(filename)
        files.append(str(path))
        offset += batch["rows"]

    if not files or offset != manifest["total_rows"]:
        raise RuntimeError("Manifest row counts do not reconcile.")

    # Avoid mixing a new run with outputs from an earlier run.
    existing_outputs = list(OUT_DIR.glob("*.parquet"))
    existing_outputs += list(EXCEL_DIR.glob("*.csv"))

    if existing_outputs:
        raise RuntimeError(
            "Processed outputs already exist. Move them into an "
            "archive folder before running a new cleaning version."
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    EXCEL_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    raw = pl.scan_csv(
        files,
        infer_schema=False,
        ignore_errors=False,
        try_parse_dates=False,
        low_memory=True,
    )

    columns = raw.collect_schema().names()
    required = set(NUMERIC_COLUMNS + [
        "source_row_id", "parid", "year", "period", "extracrdt",
        "boro", "fintaxclass", "bldg_class", "valref",
        "noav", "zip_code",
    ])

    missing = required - set(columns)
    if missing:
        raise RuntimeError(f"Required columns missing: {sorted(missing)}")

    # Trim surrounding whitespace; standardize empty text as null.
    # No title-casing, ZIP truncation, or zero-filling.
    standardized = raw.with_columns([
        pl.col(column)
        .str.strip_chars()
        .replace("", None)
        .alias(column)
        for column in columns
    ]).with_columns([
        pl.col("fintaxclass").str.to_uppercase(),
        pl.col("bldg_class").str.to_uppercase(),
    ])

    # Assign one mutually exclusive routing reason per raw record.
    routed = standardized.with_columns(
        pl.when(pl.col("period").is_null())
        .then(pl.lit("missing_period"))
        .when(pl.col("period") != "3")
        .then(pl.lit("not_final_snapshot"))
        .when(pl.col("fintaxclass").is_null())
        .then(pl.lit("missing_final_tax_class"))
        .when(~pl.col("fintaxclass").is_in(TARGET_CLASSES))
        .then(pl.lit("outside_target_tax_classes"))
        .when(
            pl.col("boro").is_null()
            | ~pl.col("boro").is_in(list(BOROUGHS))
        )
        .then(pl.lit("invalid_or_missing_borough"))
        .otherwise(pl.lit("retained"))
        .alias("routing_reason")
    )

    reconciliation = (
        routed.group_by("routing_reason")
        .agg(pl.len().alias("record_count"))
        .sort("routing_reason")
        .collect(engine="streaming")
    )

    if reconciliation["record_count"].sum() != offset:
        raise RuntimeError("Raw routing counts do not reconcile.")

    reconciliation.write_csv(REPORT_DIR / "record_routing.csv")
    print(reconciliation)

    # Save record IDs and reasons; full originals remain in raw batches.
    (
        routed.filter(pl.col("routing_reason") != "retained")
        .select([
            "source_row_id", "parid", "year", "period",
            "extracrdt", "boro", "fintaxclass", "routing_reason",
        ])
        .sink_parquet(REPORT_DIR / "excluded_record_index.parquet")
    )

    retained = routed.filter(
        pl.col("routing_reason") == "retained"
    ).drop("routing_reason")

    # Preserve original numeric text and record conversion failures.
    conversions = []
    for column in NUMERIC_COLUMNS:
        original = pl.col(column)
        converted = original.cast(pl.Float64, strict=False)

        conversions.extend([
            original.alias(f"{column}_raw"),
            (
                original.is_not_null()
                & (
                    converted.is_null()
                    | ~converted.is_finite().fill_null(False)
                )
            ).alias(f"{column}_parse_error"),
            pl.when(converted.is_finite().fill_null(False))
            .then(converted)
            .otherwise(None)
            .alias(column),
        ])

    prepared = retained.with_columns(conversions)

    # Flags preserve records while identifying limits to their use.
    prepared = prepared.with_columns([
        pl.any_horizontal([
            pl.col(f"{column}_parse_error")
            for column in NUMERIC_COLUMNS
        ]).alias("has_numeric_parse_error"),

        pl.col("parid").is_null().alias("missing_property_id"),

        (
            pl.col("year").is_null()
            | ~pl.col("year").str.contains(r"^\d{4}$")
        ).alias("invalid_or_missing_year"),

        (
            pl.col("gross_sqft").is_null()
            | (pl.col("gross_sqft") <= 0)
        ).alias("missing_or_nonpositive_sqft"),

        (
            pl.col("finacttot").is_null()
            | (pl.col("finacttot") <= 0)
        ).alias("missing_or_nonpositive_assessment"),

        pl.col("bldg_class").is_null()
        .alias("missing_building_class"),

        pl.col("valref").str.to_uppercase()
        .eq("Y").fill_null(False)
        .alias("value_reflected_in_other_lot"),

        pl.col("noav").eq("1").fill_null(False)
        .alias("building_in_progress"),

        (
            pl.col("zip_code").is_null()
            | ~pl.col("zip_code").str.contains(r"^\d{5}$")
        ).alias("zip_needs_review"),
    ])

    # Excel gets typed values and flags.
    # Parquet additionally retains original numeric text.
    excel_columns = [
        column
        for column in prepared.collect_schema().names()
        if not column.endswith("_raw")
        and not column.endswith("_parse_error")
    ]

    export_counts = []
    flag_counts = []
    total_exported = 0

    for code, name in BOROUGHS.items():
        slug = name.lower().replace(" ", "_")
        parquet_path = OUT_DIR / f"{slug}.parquet"

        print(f"\nPreparing {name}...", flush=True)

        (
            prepared.filter(pl.col("boro") == code)
            .with_columns(pl.lit(name).alias("borough_name"))
            .sink_parquet(parquet_path)
        )

        # Read one borough at a time, rather than collecting all history.
        borough = pl.scan_parquet(parquet_path)

        yearly_counts = (
            borough.group_by("year")
            .agg(pl.len().alias("record_count"))
            .sort("year", nulls_last=True)
            .collect(engine="streaming")
        )

        flag_columns = [
            column
            for column, dtype in borough.collect_schema().items()
            if dtype == pl.Boolean
        ]

        flags = (
            borough.select([
                pl.col(column).sum().alias(column)
                for column in flag_columns
            ])
            .collect(engine="streaming")
            .row(0, named=True)
        )
        flag_counts.append({"borough": name, **flags})

        for record in yearly_counts.iter_rows(named=True):
            year = record["year"]
            count = record["record_count"]

            # Invalid years remain in Parquet for review.
            if year is None or len(year) != 4 or not year.isdigit():
                print(f"Review required: {count:,} records with year {year!r}")
                continue

            if count > MAX_EXCEL_DATA_ROWS:
                raise RuntimeError(
                    f"{name} {year}: {count:,} rows exceed Excel's limit."
                )

            csv_path = EXCEL_DIR / f"{slug}_{year}.csv"

            (
                borough.filter(pl.col("year") == year)
                .select(excel_columns + ["borough_name"])
                .sink_csv(csv_path)
            )

            export_counts.append({
                "borough": name,
                "year": year,
                "record_count": count,
                "file": csv_path.name,
            })
            total_exported += count

            print(f"Exported {csv_path.name}: {count:,} records")

    if export_counts:
        pl.DataFrame(export_counts).write_csv(
            REPORT_DIR / "excel_export_counts.csv"
        )

    pl.DataFrame(flag_counts).write_csv(
        REPORT_DIR / "quality_flag_counts.csv"
    )

    print("\nPreparation finished.")
    print(f"Excel records exported: {total_exported:,}")
    print(f"Processed files: {OUT_DIR}")
    print(f"Cleaning reports: {REPORT_DIR}")
    print("Review flags and reconcile outputs before analysis.")


if __name__ == "__main__":
    main()
