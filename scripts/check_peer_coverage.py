from pathlib import Path
import csv

import duckdb


PROJECT_DIR = Path(__file__).resolve().parents[1]
SOURCE = (
    PROJECT_DIR
    / "data/processed/analysis/comparison_candidates.parquet"
)
REPORT_DIR = PROJECT_DIR / "reports/peer_coverage"


def save_report(connection, filename, sql):
    result = connection.execute(sql)
    columns = [item[0] for item in result.description]
    rows = result.fetchall()

    with (REPORT_DIR / filename).open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"\n{filename}")
    for row in rows[:20]:
        print(dict(zip(columns, row)))


def main():
    if not SOURCE.is_file():
        raise FileNotFoundError(SOURCE)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as connection:
        connection.read_parquet(str(SOURCE)).create_view("candidates")

        # Trial bands only: we will assess their usefulness.
        connection.execute("""
            CREATE TEMP VIEW coverage_input AS
            SELECT *,
                CASE
                    WHEN gross_sqft < 5000 THEN '01_under_5000'
                    WHEN gross_sqft < 10000 THEN '02_5000_to_9999'
                    WHEN gross_sqft < 20000 THEN '03_10000_to_19999'
                    WHEN gross_sqft < 40000 THEN '04_20000_to_39999'
                    WHEN gross_sqft < 80000 THEN '05_40000_to_79999'
                    WHEN gross_sqft < 160000 THEN '06_80000_to_159999'
                    ELSE '07_160000_plus'
                END AS trial_size_band,
                COALESCE(
                    REGEXP_FULL_MATCH(zip_code, '[0-9]{5}')
                    AND zip_code <> '00000',
                    FALSE
                ) AS zip_format_usable
            FROM candidates
        """)

        save_report(connection, "size_distributions.csv", """
            SELECT
                year, property_group, bldg_class, fintaxclass,
                COUNT(*) AS records,
                MIN(gross_sqft) AS min_sqft,
                QUANTILE_CONT(gross_sqft, 0.10) AS p10_sqft,
                MEDIAN(gross_sqft) AS median_sqft,
                QUANTILE_CONT(gross_sqft, 0.90) AS p90_sqft,
                MAX(gross_sqft) AS max_sqft
            FROM coverage_input
            GROUP BY year, property_group, bldg_class, fintaxclass
            ORDER BY year, property_group, bldg_class, fintaxclass
        """)

        # Window counts include the subject property.
        # 11 group members = 10 other properties.
        connection.execute("""
            CREATE TEMP TABLE coverage AS
            SELECT *,
                COUNT(*) OVER (
                    PARTITION BY
                        year, boro, zip_code, fintaxclass, bldg_class
                ) AS group_count_without_size,
                COUNT(*) OVER (
                    PARTITION BY
                        year, boro, zip_code, fintaxclass, bldg_class,
                        trial_size_band
                ) AS group_count_with_size
            FROM coverage_input
            WHERE zip_format_usable
        """)

        save_report(connection, "coverage_summary.csv", """
            WITH totals AS (
                SELECT
                    year, property_group,
                    COUNT(*) AS candidate_records,
                    COUNT(*) FILTER (
                        WHERE NOT zip_format_usable
                    ) AS zip_unusable_records,
                    COUNT(*) FILTER (
                        WHERE age_needs_review
                    ) AS age_review_records,
                    COUNT(*) FILTER (
                        WHERE size_details_need_review
                    ) AS size_review_records
                FROM coverage_input
                GROUP BY year, property_group
            ),
            covered AS (
                SELECT
                    year, property_group,
                    COUNT(*) FILTER (
                        WHERE group_count_without_size >= 11
                    ) AS enough_peers_without_size,
                    COUNT(*) FILTER (
                        WHERE group_count_with_size >= 11
                    ) AS enough_peers_with_size
                FROM coverage
                GROUP BY year, property_group
            )
            SELECT
                t.*,
                COALESCE(c.enough_peers_without_size, 0)
                    AS enough_peers_without_size,
                COALESCE(c.enough_peers_with_size, 0)
                    AS enough_peers_with_size,
                ROUND(
                    100.0 * COALESCE(c.enough_peers_with_size, 0)
                    / t.candidate_records, 2
                ) AS pct_candidates_with_size_peers
            FROM totals t
            LEFT JOIN covered c USING (year, property_group)
            ORDER BY year, property_group
        """)

        save_report(connection, "trial_peer_groups.csv", """
            SELECT
                year, borough_name, zip_code, property_group,
                fintaxclass, bldg_class, trial_size_band,
                COUNT(*) AS group_records,
                COUNT(*) - 1 AS other_peers_per_property,
                MIN(gross_sqft) AS min_sqft,
                MAX(gross_sqft) AS max_sqft
            FROM coverage
            GROUP BY
                year, borough_name, zip_code, property_group,
                fintaxclass, bldg_class, trial_size_band
            ORDER BY year, borough_name, property_group, group_records DESC
        """)

    print(f"\nReports saved to: {REPORT_DIR}")
    print("No candidate data or Excel exports were modified.")


if __name__ == "__main__":
    main()
