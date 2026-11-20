import csv
import hashlib
import io
import json
import os
import time
from pathlib import Path

import duckdb
import requests

from columns import KEEP_COLS


DATASET_ID = "8y4t-faws"
BASE_URL = "https://data.cityofnewyork.us"
API_URL = f"{BASE_URL}/resource/{DATASET_ID}.json"
METADATA_URL = f"{BASE_URL}/api/views/{DATASET_ID}.json"

# Resolve paths from the project, regardless of the terminal directory.
PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "data/raw/assessment_batches"
MANIFEST_PATH = OUTPUT_DIR / "manifest.json"

BATCH_SIZE = 20_000
MAX_ATTEMPTS = 6

# Keep every available year and snapshot of ordinary real estate.
WHERE = "rectype = 1"

# One extra technical column supports duplicate detection.
OUTPUT_COLUMNS = ["source_row_id"] + KEEP_COLS

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

session = requests.Session()
session.headers.update({
    "User-Agent": "NYC-Property-Data-Project/1.0",
    "Accept": "application/json",
})

# Optional: configure this environment variable if you have an app token.
token = os.environ.get("SOCRATA_APP_TOKEN")
if token:
    session.headers["X-App-Token"] = token


def request_json(url, params=None):
    """Retry temporary HTTP and connection failures."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with session.get(
                url,
                params=params,
                timeout=(30, 180),
            ) as response:
                response.raise_for_status()
                return response.json()

        except (requests.RequestException, ValueError) as error:
            status = (
                error.response.status_code
                if isinstance(error, requests.HTTPError)
                and error.response is not None
                else None
            )

            # Authentication and invalid queries need intervention.
            if status is not None and status not in {
                408, 429, 500, 502, 503, 504
            }:
                raise

            if attempt == MAX_ATTEMPTS:
                raise

            delay = min(60, 2 ** attempt)
            print(
                f"Request failed ({error}). "
                f"Retry {attempt}/{MAX_ATTEMPTS - 1} "
                f"in {delay}s...",
                flush=True,
            )
            time.sleep(delay)


def source_info():
    metadata = request_json(METADATA_URL)

    # These describe data changes, not merely descriptive metadata edits.
    version = {
        key: metadata[key]
        for key in ("rowsUpdatedAt", "dataUpdatedAt")
        if metadata.get(key) is not None
    }

    if not version:
        raise RuntimeError(
            "No source data timestamp available; "
            "cannot safely verify resumability."
        )

    return metadata, version


def check_source(expected_version):
    _, current_version = source_info()
    if current_version != expected_version:
        raise RuntimeError(
            "Source data changed. Download stopped to avoid mixing "
            "versions. Keep this folder and use a new output folder "
            "for a fresh download."
        )


def source_count():
    result = request_json(
        API_URL,
        params={
            "$select": "count(*) AS total",
            "$where": WHERE,
        },
    )
    return int(result[0]["total"])


def save_manifest(manifest):
    temporary = MANIFEST_PATH.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )
    temporary.replace(MANIFEST_PATH)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    start = time.monotonic()
    metadata, version = source_info()

    available = {
        column["fieldName"]
        for column in metadata["columns"]
        if "fieldName" in column
    }
    missing = sorted(set(KEEP_COLS) - available)
    if missing:
        raise RuntimeError(f"API columns not found: {missing}")

    config = {
        "dataset": DATASET_ID,
        "columns": OUTPUT_COLUMNS,
        "where": WHERE,
        "order": ":id ASC",
        "batch_size": BATCH_SIZE,
    }

    total_rows = source_count()
    check_source(version)

    if MANIFEST_PATH.exists():
        manifest = json.loads(
            MANIFEST_PATH.read_text(encoding="utf-8")
        )
        if manifest["config"] != config:
            raise RuntimeError(
                "Download settings changed. Use a new output folder."
            )
        if manifest["version"] != version:
            raise RuntimeError(
                "Source changed since the previous run. "
                "Use a new output folder."
            )
        if manifest["total_rows"] != total_rows:
            raise RuntimeError("Source row count changed.")
    else:
        if any(OUTPUT_DIR.glob("batch_*.csv")):
            raise RuntimeError(
                "Batch files exist without a manifest. "
                "Use a new output folder."
            )

        manifest = {
            "config": config,
            "version": version,
            "total_rows": total_rows,
            "batches": [],
            "verified": False,
        }
        save_manifest(manifest)

    # Confirm saved batches are intact before resuming.
    offset = 0
    for batch in manifest["batches"]:
        path = OUTPUT_DIR / batch["file"]
        if (
            batch["offset"] != offset
            or not path.is_file()
            or sha256(path) != batch["sha256"]
        ):
            raise RuntimeError(
                f"Saved batch failed validation: {path}"
            )
        offset += batch["rows"]

    print(f"Source records: {total_rows:,}")
    print(f"Already saved:  {offset:,}")
    print(f"Output folder:  {OUTPUT_DIR}", flush=True)

    while offset < total_rows:
        expected_rows = min(BATCH_SIZE, total_rows - offset)

        records = request_json(
            API_URL,
            params={
                "$select": (
                    ":id AS source_row_id," + ",".join(KEEP_COLS)
                ),
                "$where": WHERE,
                "$order": ":id ASC",
                "$limit": expected_rows,
                "$offset": offset,
            },
        )

        if not isinstance(records, list):
            raise RuntimeError("API returned an unexpected response.")

        if len(records) != expected_rows:
            raise RuntimeError(
                f"Expected {expected_rows:,} records at offset "
                f"{offset:,}; received {len(records):,}."
            )

        ids = [record.get("source_row_id") for record in records]
        if any(value is None for value in ids):
            raise RuntimeError("API row identifiers are missing.")
        if len(set(ids)) != len(ids):
            raise RuntimeError("Duplicate API row IDs within a batch.")

        # A changed source invalidates offset-based pagination.
        check_source(version)

        filename = f"batch_{offset:012d}.csv"
        destination = OUTPUT_DIR / filename
        temporary = destination.with_suffix(".csv.tmp")

        # Build and validate this small batch before saving.
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(
            buffer,
            fieldnames=OUTPUT_COLUMNS,
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(records)
        payload = buffer.getvalue().encode("utf-8")

        temporary.write_bytes(payload)
        temporary.replace(destination)

        manifest["batches"].append({
            "file": filename,
            "offset": offset,
            "rows": len(records),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })
        manifest["verified"] = False
        save_manifest(manifest)

        offset += len(records)
        elapsed = (time.monotonic() - start) / 60
        print(
            f"Saved {offset:,}/{total_rows:,} records "
            f"({offset / total_rows:.1%}) | "
            f"This run: {elapsed:.1f} min",
            flush=True,
        )
        time.sleep(0.5)

    print("\nVerifying all saved batches...", flush=True)

    if total_rows:
        # Validate only files recorded in this manifest.
        files = [
            str(OUTPUT_DIR / batch["file"])
            for batch in manifest["batches"]
        ]
        with duckdb.connect() as connection:
            relation = connection.read_csv(
                files,
                header=True,
                all_varchar=True,
            )
            relation.create_view("downloaded")
            rows, unique_ids = connection.execute("""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT source_row_id)
                FROM downloaded
            """).fetchone()
    else:
        rows = unique_ids = 0

    if rows != total_rows or unique_ids != total_rows:
        raise RuntimeError(
            f"Verification failed: rows={rows:,}, "
            f"unique API IDs={unique_ids:,}, "
            f"expected={total_rows:,}."
        )

    if source_count() != total_rows:
        raise RuntimeError("Source count changed during verification.")
    check_source(version)

    manifest["verified"] = True
    save_manifest(manifest)

    print(f"\nVerified: {rows:,} records with unique API IDs.")
    print("Download complete. Raw batch files are ready for inspection.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped. Rerun the same command to resume saved batches.")
        raise SystemExit(130)
    except Exception:
        print(
            "\nStopped because of an error. Completed batches remain saved.",
            flush=True,
        )
        raise
    finally:
        session.close()
