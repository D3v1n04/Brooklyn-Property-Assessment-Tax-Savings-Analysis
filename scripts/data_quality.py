import csv
import json
from pathlib import Path

import duckdb

from clean_and_split import NUMERIC_COLUMNS


PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data/raw/assessment_batches"
PROCESSED_DIR = PROJECT_DIR / "data/processed"
EXCEL_DIR = PROCESSED_DIR / "excel"
REPORT_DIR = PROJECT_DIR / "reports/data_quality"

BOROUGHS = {
    "manhattan": "1",
    "bronx": "2",
    "brooklyn": "3",
    "queens": "4",
    "staten_island": "5",
}

FINANCIAL_COLUMNS = [
    "finmkttot",
    "finacttot",
    "finactextot",
    "fintrntot",
    "fintrnextot",
    "fintxbtot",
    "fintxbextot",
]

checks = []


def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'


def clean_text(column):
    return f"NULLIF(TRIM({quote_identifier(column)}), '')"


def report(connection, name, sql):
    """Save a small query result without requiring pandas."""
    result = connection.execute(sql)
    headers = [column[0] for column in result.description]
    rows = result.fetchall()

    with (REPORT_DIR / f"{name}.csv").open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"\n{name}")
    for row in rows[:15]:
        print(dict(zip(headers, row)))

    return rows


def check(name, actual, expected=0):
    status = "PASS" if actual == expected else "FAIL"
    checks.append({
        "check": name,
        "status": status,
        "actual": actual,
        "expected": expected,
    })
    print(f"{status}: {name} | actual={actual} expected={expected}")


def scalar(connection, sql):
    return connection.execute(sql).fetchone()[0]


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(
        (RAW_DIR / "manifest.json").read_text(encoding="utf-8")
    )
    if manifest.get("verified") is not True:
        raise RuntimeError("Download is not marked as verified.")

    raw_files = [
        str(RAW_DIR / batch["file"])
        for batch in manifest["batches"]
    ]
    parquet_files = [
        str(PROCESSED_DIR / f"{name}.parquet")
        for name in BOROUGHS
    ]

    for filename in raw_files + parquet_files:
        if not Path(filename).is_file():
            raise FileNotFoundError(filename)

    with duckdb.connect() as connection:
        connection.execute("SET memory_limit = '8GB'")

        connection.read_csv(
            raw_files,
            header=True,
            all_varchar=True,
            sep=",",
            quotechar='"',
            escapechar='"',
        ).create_view("raw")

        connection.read_parquet(
            parquet_files
        ).create_view("processed")

        # Independently reproduce the source inclusion criteria.
        # No year restriction and no positive-value filter.
        columns = manifest["config"]["columns"]
        normalized = []

        for column in columns:
            expression = clean_text(column)
            if column in {"fintaxclass", "bldg_class"}:
                expression = f"UPPER({expression})"

            normalized.append(
                f"{expression} AS {quote_identifier(column)}"
            )

        connection.execute(
            "CREATE TEMP VIEW normalized_raw AS SELECT "
            + ", ".join(normalized)
            + " FROM raw"
        )

        print("Selecting eligible raw records...", flush=True)
        connection.execute("""
            CREATE TEMP TABLE eligible_raw AS
            SELECT *
            FROM normalized_raw
            WHERE period = '3'
              AND fintaxclass IN ('2', '2A', '2B', '2C', '4')
              AND boro IN ('1', '2', '3', '4', '5')
        """)

        raw_count = scalar(connection, "SELECT COUNT(*) FROM raw")
        expected_count = scalar(
            connection, "SELECT COUNT(*) FROM eligible_raw"
        )
        processed_count = scalar(
            connection, "SELECT COUNT(*) FROM processed"
        )

        check("Raw count matches manifest",
              raw_count, manifest["total_rows"])
        check("Processed count matches eligible raw",
              processed_count, expected_count)

        for table in ("eligible_raw", "processed"):
            duplicate_ids = scalar(connection, f"""
                SELECT COUNT(*) FROM (
                    SELECT source_row_id
                    FROM {table}
                    GROUP BY source_row_id
                    HAVING COUNT(*) > 1
                )
            """)
            check(f"{table}: duplicate API ID groups", duplicate_ids)

            missing_ids = scalar(connection, f"""
                SELECT COUNT(*) FROM {table}
                WHERE source_row_id IS NULL
                   OR TRIM(source_row_id) = ''
            """)
            check(f"{table}: missing API IDs", missing_ids)

        check("Eligible raw IDs missing from processed",
              scalar(connection, """
                  SELECT COUNT(*) FROM (
                      SELECT source_row_id FROM eligible_raw
                      EXCEPT
                      SELECT source_row_id FROM processed
                  )
              """))

        check("Unexpected processed IDs",
              scalar(connection, """
                  SELECT COUNT(*) FROM (
                      SELECT source_row_id FROM processed
                      EXCEPT
                      SELECT source_row_id FROM eligible_raw
                  )
              """))

        report(connection, "unexpected_processed_records", """
            SELECT
                p.source_row_id,
                p.parid,
                p.year,
                r.period AS raw_period,
                p.period AS processed_period,
                r.fintaxclass AS raw_tax_class,
                p.fintaxclass AS processed_tax_class,
                LENGTH(r.fintaxclass) AS raw_tax_class_length,
                r.boro AS raw_borough,
                p.boro AS processed_borough
            FROM processed p
            LEFT JOIN raw r
                ON p.source_row_id = r.source_row_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM eligible_raw e
                WHERE e.source_row_id = p.source_row_id
            )
            ORDER BY p.year, p.parid
        """)

        # Compare all 56 downloaded fields, row by row.
        # Numeric fields are compared to the documented Float64
        # conversion; invalid/nonfinite inputs become null.
        comparisons = []

        for column in columns:
            q = quote_identifier(column)
            if column in NUMERIC_COLUMNS:
                parsed = f"TRY_CAST(r.{q} AS DOUBLE)"
                expected = (
                    f"CASE WHEN ISFINITE({parsed}) "
                    f"THEN {parsed} ELSE NULL END"
                )
            else:
                expected = f"r.{q}"

            comparisons.append(
                f"COUNT(*) FILTER (WHERE p.{q} "
                f"IS DISTINCT FROM {expected}) AS {q}"
            )

        mismatches = connection.execute(
            "SELECT " + ", ".join(comparisons)
            + " FROM eligible_raw r JOIN processed p "
            + "USING (source_row_id)"
        ).fetchone()

        for column, count in zip(columns, mismatches):
            check(f"Source value preserved: {column}", count)

        # Full-precision financial reconciliation, plus null counts.
        raw_sums = []
        processed_sums = []

        for column in FINANCIAL_COLUMNS:
            q = quote_identifier(column)
            parsed = f"TRY_CAST({q} AS DOUBLE)"
            raw_sums.append(
                f"SUM(CASE WHEN ISFINITE({parsed}) THEN {parsed} END)"
            )
            processed_sums.append(f"SUM({q})")

        raw_totals = connection.execute(
            "SELECT " + ", ".join(raw_sums)
            + " FROM eligible_raw"
        ).fetchone()
        processed_totals = connection.execute(
            "SELECT " + ", ".join(processed_sums)
            + " FROM processed"
        ).fetchone()

        with (REPORT_DIR / "financial_totals.csv").open(
            "w", encoding="utf-8", newline=""
        ) as file:
            writer = csv.writer(file)
            writer.writerow([
                "column", "eligible_raw_total", "processed_total",
                "difference",
            ])
            for column, raw_total, output_total in zip(
                FINANCIAL_COLUMNS, raw_totals, processed_totals
            ):
                difference = (
                    output_total - raw_total
                    if output_total is not None and raw_total is not None
                    else None
                )
                writer.writerow([
                    column, raw_total, output_total, difference
                ])

        # Repeated property-year keys need review, not deletion.
        report(connection, "repeated_property_years", """
            SELECT parid, year, COUNT(*) AS record_count
            FROM processed
            GROUP BY parid, year
            HAVING COUNT(*) > 1
            ORDER BY record_count DESC, year, parid
        """)

        report(connection, "borough_year_summary", """
            SELECT
                borough_name,
                year,
                COUNT(*) AS records,
                COUNT(DISTINCT parid) AS distinct_property_ids,
                SUM(finmkttot) AS market_value,
                SUM(finacttot) AS actual_assessed_value
            FROM processed
            GROUP BY borough_name, year
            ORDER BY borough_name, year
        """)

        # These are review flags, not legal determinations.
        report(connection, "flagged_by_property_type", """
            SELECT
                borough_name, year, fintaxclass, bldg_class,
                COUNT(*) AS records,
                COUNT(*) FILTER (
                    WHERE missing_or_nonpositive_sqft
                ) AS sqft_flagged,
                COUNT(*) FILTER (
                    WHERE missing_or_nonpositive_assessment
                ) AS assessment_flagged,
                COUNT(*) FILTER (
                    WHERE zip_needs_review
                ) AS zip_flagged,
                COUNT(*) FILTER (
                    WHERE value_reflected_in_other_lot
                ) AS value_in_other_lot
            FROM processed
            GROUP BY borough_name, year, fintaxclass, bldg_class
            ORDER BY sqft_flagged DESC, zip_flagged DESC
        """)

        report(connection, "flagged_examples", """
            SELECT
                source_row_id, parid, borough_name, year,
                fintaxclass, bldg_class, gross_sqft, finacttot,
                zip_code, valref,
                missing_or_nonpositive_sqft,
                missing_or_nonpositive_assessment,
                zip_needs_review
            FROM processed
            WHERE missing_or_nonpositive_sqft
               OR missing_or_nonpositive_assessment
               OR zip_needs_review
               OR value_reflected_in_other_lot
            ORDER BY borough_name, year, parid
            LIMIT 100
        """)

        # Check each Excel file against the corresponding Parquet rows.
        expected_csv_names = set()
        schema = connection.execute(
            "DESCRIBE processed"
        ).fetchall()
        types = {row[0]: row[1] for row in schema}

        partitions = connection.execute("""
            SELECT boro, year, COUNT(*)
            FROM processed
            GROUP BY boro, year
        """).fetchall()

        slug_for_code = {
            code: slug for slug, code in BOROUGHS.items()
        }

        print("\nChecking Excel exports...", flush=True)

        for boro, year, expected_rows in partitions:
            filename = f"{slug_for_code[boro]}_{year}.csv"
            expected_csv_names.add(filename)
            path = EXCEL_DIR / filename

            if not path.is_file():
                check(f"CSV exists: {filename}", 0, 1)
                continue

            connection.read_csv(
                str(path),
                header=True,
                all_varchar=True,
                sep=",",
                quotechar='"',
                escapechar='"',
            ).create_view("excel_file", replace=True)

            csv_columns = [
                row[0]
                for row in connection.execute(
                    "DESCRIBE excel_file"
                ).fetchall()
            ]
            expected_csv_columns = [
                column for column in types
                if not column.endswith("_raw")
                and not column.endswith("_parse_error")
            ]

            check(
                f"CSV columns: {filename}",
                csv_columns == expected_csv_columns,
                True,
            )
            if csv_columns != expected_csv_columns:
                continue

            csv_count = scalar(
                connection, "SELECT COUNT(*) FROM excel_file"
            )
            check(f"CSV row count: {filename}",
                  csv_count, expected_rows)
            check(f"CSV fits Excel: {filename}",
                  csv_count <= 1_048_575, True)

            # Strict casts fail visibly if CSV values became invalid.
            projection = ", ".join(
                f"CAST({quote_identifier(column)} AS {types[column]}) "
                f"AS {quote_identifier(column)}"
                for column in csv_columns
            )
            selected = ", ".join(
                quote_identifier(column) for column in csv_columns
            )

            connection.execute(
                "CREATE OR REPLACE TEMP VIEW typed_excel AS SELECT "
                + projection + " FROM excel_file"
            )

            # Parameters are values, not interpolated SQL text.
            connection.execute("""
                CREATE OR REPLACE TEMP TABLE expected_partition AS
                SELECT * FROM processed WHERE boro = ? AND year = ?
            """, [boro, year])

            for left, right, direction in [
                ("typed_excel", "expected_partition", "extra_or_changed"),
                ("expected_partition", "typed_excel", "missing_or_changed"),
            ]:
                difference = scalar(connection, f"""
                    SELECT COUNT(*) FROM (
                        SELECT {selected} FROM {left}
                        EXCEPT ALL
                        SELECT {selected} FROM {right}
                    )
                """)
                check(f"{filename}: {direction}", difference)

        actual_names = {
            path.name for path in EXCEL_DIR.glob("*.csv")
        }
        check("Unexpected CSV files",
              len(actual_names - expected_csv_names))
        check("Missing CSV files",
              len(expected_csv_names - actual_names))

    with (REPORT_DIR / "check_summary.csv").open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=["check", "status", "actual", "expected"]
        )
        writer.writeheader()
        writer.writerows(checks)

    failures = sum(row["status"] == "FAIL" for row in checks)
    print(f"\nCompleted: {len(checks)} checks, {failures} failures.")
    print(f"Reports: {REPORT_DIR}")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
