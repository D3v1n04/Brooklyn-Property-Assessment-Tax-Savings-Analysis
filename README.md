# Brooklyn Property Assessment Analysis

Python data preparation and Excel analysis of Brooklyn property assessments, using data quality checks and peer benchmarks to identify properties for further review.

## Download the Excel Workbook

[Download the workbook from the v1.0 release](https://github.com/D3v1n04/Brooklyn-Property-Assessment-Tax-Savings-Analysis/releases/tag/v1.0)

Under **Assets**, select **Brooklyn_Property_Assessment_Analysis.xlsx** (approximately 114 MB).

Open the downloaded file in Microsoft Excel. Start with **Guide** and **Assumptions**, then explore **Dashboard** and **Review_List**.

The workbook is distributed as a release attachment because of its size. It is not included in the repository's source-code ZIP downloads.

## Results

- Analyzed 161,962 property-year records.
- Retained 155,816 records after ZIP and size quality checks (96.21%).
- Benchmarked 27,707 of 32,478 FY2027 records.
- Identified 3,978 FY2027 review candidates: 14.36% of benchmarked records.

Review candidates are screening results, not confirmed cases of overassessment or estimates of tax savings.

## Tools

- Python for data collection, cleaning, and preparation
- Excel tables, PivotTables, XLOOKUP, LET, FILTER, SORT, and dynamic arrays
- Excel dashboard and prioritized review list

## Methodology

1. Collect and prepare property assessment records.
2. Flag records requiring ZIP or size review.
3. Group properties by fiscal year, ZIP code, building class, tax class, and gross-square-footage band.
4. Require at least 10 other eligible peers for benchmarking.
5. Calculate assessed value per square foot and the peer median, excluding the subject property.
6. Calculate the percentage above the peer median and identify review candidates using the workbook's screening threshold.

## Validation

- Workbook completeness checks returned PASS for group benchmarks, property benchmark lookups, percentage comparisons, and final peer comparisons.
- An independent check of the top FY2027 candidate reproduced its peer median of $79.15 per square foot.
- The candidate's group contained 27 properties, corresponding to 26 other peers.

These checks support the reviewed calculations; they are not a full independent audit of every workbook cell.

## Key Limitation

Properties within the same size band can still differ substantially.

For example, the top candidate had 650 square feet, while its eligible peers ranged from 918 to 4,420 square feet, with a median of 2,542.5. This limits comparability and requires further review before interpreting the assessment gap.

Peer comparisons do not control for every property characteristic, valuation factor, or tax treatment.

## Repository Structure

- scripts/ — Python collection, inspection, cleaning, and preparation scripts
- data/ — project data files
- excel/ — Brooklyn_Property_Assessment_Analysis.xlsx
- reports/ — supporting outputs

## Viewing the Analysis

Open the workbook in Microsoft Excel. Start with the Guide and Assumptions sheets, then review Dashboard and Review_List. Supporting calculations appear in Properties and Peer_Analysis.

## Scope

This project covers Brooklyn properties. Its findings should not be generalized to all New York City properties.