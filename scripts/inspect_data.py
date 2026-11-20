import csv
import hashlib
import json
import time
from pathlib import Path

import polars as pl


PROJECT_DIR = Path(__file__).resolve().parents[1]
BATCH_DIR = PROJECT_DIR / "data/raw/assessment_batches"
REPORT_DIR = PROJECT_DIR / "reports/data_profile"


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_dataset():
    """Inspect downloaded batches without modifying the raw data."""
    start = time.monotonic()
    manifest_path = BATCH_DIR / "manifest.json"

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    if manifest.get("verified") is not True:
        raise RuntimeError(
            "The downloader has not marked this download as verified."
        )

    batches = manifest["batches"]
    expected_columns = manifest["config"]["columns"]
    expected_rows = manifest["total_rows"]

    if not batches:
        raise RuntimeError("No batches recorded in the manifest.")

    files = []
    manifest_rows = 0
    total_bytes = 0
    seen_files = set()

    print("Checking saved batches...", flush=True)

    for batch in batches:
        filename = batch["file"]

        if filename in seen_files:
            raise RuntimeError(f"Repeated batch filename: {filename}")
        seen_files.add(filename)

        path = BATCH_DIR / filename

        if batch["offset"] != manifest_rows:
            raise RuntimeError(f"Unexpected batch offset: {filename}")

        if not path.is_file():
            raise FileNotFoundError(path)

        if file_hash(path) != batch["sha256"]:
            raise RuntimeError(f"File checksum mismatch: {filename}")

        with path.open("r", encoding="utf-8", newline="") as file:
            header = next(csv.reader(file))

        if header != expected_columns:
            raise RuntimeError(f"Column mismatch: {filename}")

        files.append(str(path))
        manifest_rows += batch["rows"]
        total_bytes += path.stat().st_size

    if manifest_rows != expected_rows:
        raise RuntimeError("Manifest batch counts do not reconcile.")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Batch files: {len(files):,}")
    print(f"Total size: {total_bytes / 1024**3:.2f} GiB")
    print("Reading CSV fields as text...", flush=True)

    lf = pl.scan_csv(
        files,
        infer_schema=False,
        ignore_errors=False,
        try_parse_dates=False,
        low_memory=True,
    )

    columns = lf.collect_schema().names()

    # Calculate row count and missing values in one aggregation.
    expressions = [pl.len().alias("total_rows")]

    for i, column in enumerate(columns):
        expressions.extend([
            pl.col(column).null_count().alias(f"null_{i}"),
            (
                pl.col(column)
                .str.strip_chars()
                .eq("")
                .fill_null(False)
                .sum()
                .alias(f"blank_{i}")
            ),
        ])

    print("Counting records and missing values...", flush=True)
    stats = (
        lf.select(expressions)
        .collect(engine="streaming")
        .row(0, named=True)
    )

    total_rows = stats["total_rows"]

    if total_rows != expected_rows:
        raise RuntimeError(
            f"Expected {expected_rows:,} rows; found {total_rows:,}."
        )

    records = []
    for i, column in enumerate(columns):
        nulls = stats[f"null_{i}"]
        blanks = stats[f"blank_{i}"]
        missing = nulls + blanks

        records.append({
            "column": column,
            "read_dtype": "String",
            "null_count": nulls,
            "blank_count": blanks,
            "missing_count": missing,
            "missing_pct": (
                round(100 * missing / total_rows, 4)
                if total_rows else 0.0
            ),
        })

    summary = pl.DataFrame(records)
    summary.write_csv(REPORT_DIR / "column_summary.csv")

    print(f"\nTotal records: {total_rows:,}")
    print(f"Total columns: {len(columns)}")
    print("\n15 columns with the most missing values:")
    print(summary.sort("missing_count", descending=True).head(15))

    preview = lf.head(5).collect(engine="streaming")
    preview.write_csv(REPORT_DIR / "sample_rows.csv")

    # Independent distributions, plus combined snapshot coverage.
    breakdowns = {
        "by_year": ["year"],
        "by_borough": ["boro"],
        "by_record_type": ["rectype"],
        "by_period": ["period"],
        "by_final_tax_class": ["fintaxclass"],
        "by_building_class": ["bldg_class"],
        "by_snapshot": ["year", "period", "extracrdt"],
    }

    for name, group_columns in breakdowns.items():
        missing = set(group_columns) - set(columns)
        if missing:
            raise RuntimeError(f"Required columns missing: {missing}")

        print(f"\nCreating {name}...", flush=True)

        table = (
            lf.group_by(group_columns)
            .agg(pl.len().alias("record_count"))
            .sort(group_columns, nulls_last=True)
            .collect(engine="streaming")
        )

        if table["record_count"].sum() != total_rows:
            raise RuntimeError(f"Counts do not reconcile for {name}.")

        table.write_csv(REPORT_DIR / f"{name}.csv")

        with pl.Config(tbl_rows=30, tbl_cols=8):
            print(table.head(30))

        if table.height > 30:
            print(f"Full {table.height:,}-row table saved to CSV.")

    # Count repetition within each snapshot, not just across years.
    # This is a diagnostic key, not an established unique record key.
    print("\nChecking property repetition by snapshot...", flush=True)

    key_counts = (
        lf.group_by(["parid", "year", "period", "extracrdt"])
        .agg(pl.len().alias("records_per_key"))
    )

    repetition = (
        key_counts.select([
            pl.len().alias("candidate_key_groups"),
            (pl.col("records_per_key") > 1)
            .sum()
            .alias("repeated_key_groups"),
            (pl.col("records_per_key") - 1)
            .sum()
            .alias("records_beyond_first_per_key"),
        ])
        .collect(engine="streaming")
    )

    repetition.write_csv(
        REPORT_DIR / "property_snapshot_repetition_summary.csv"
    )
    print(repetition)

    elapsed = time.monotonic() - start
    notes = [
        f"Source manifest: {manifest_path}",
        f"Batch files: {len(files)}",
        f"Records: {total_rows}",
        f"Columns: {len(columns)}",
        f"Total bytes: {total_bytes}",
        f"Polars version: {pl.__version__}",
        f"Elapsed minutes: {elapsed / 60:.2f}",
        "Batch checksums and headers matched the manifest.",
        "All CSV columns were read as text.",
        "Missing = null plus non-null empty/whitespace strings.",
        "Text placeholders such as N/A were not counted as missing.",
        "Sample contains the first five records, not a random sample.",
        "Repeated candidate keys were counted, not removed.",
        "Candidate key: parid + year + period + extracrdt.",
        "No raw records were modified or deleted.",
        "This is a structural profile, not complete business validation.",
    ]

    (REPORT_DIR / "profile_summary.txt").write_text(
        "\n".join(notes),
        encoding="utf-8",
    )

    print(f"\nFinished in {elapsed / 60:.1f} minutes.")
    print(f"Reports saved to: {REPORT_DIR}")


if __name__ == "__main__":
    inspect_dataset()
