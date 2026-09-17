# Cloud Retail Data Engineering Pipeline

An end-to-end retail data engineering pipeline built on a medallion
(Bronze / Silver / Gold) architecture, using **Python, PySpark,
Databricks, AWS S3, and Google BigQuery**.

> **Note on data:** All data in this project (customers, products, and
> orders) is **synthetic sample data** generated for demonstration and
> testing purposes. No real customer, order, or business data is used.

## Status: Completed

This project implements the full pipeline described below — ingestion,
cleaning/validation, business aggregation, cloud orchestration, and
automated testing.

## Architecture overview

```
AWS S3 (raw retail CSVs)
   │  governed access via a Databricks Unity Catalog volume / external location
   ▼
Bronze  — raw ingestion into Delta tables
   ▼
Silver  — cleaning, validation, deduplication, foreign-key checks
          (invalid rows are captured in separate rejected-record tables,
           never silently dropped)
   ▼
Gold    — business-ready aggregates:
          product_performance, category_performance,
          customer_summary, overall_metrics
   ▼
Google BigQuery — retail analytics tables, queried via sql/analytics.sql
```

Orchestration runs as a **Databricks Job** with two tasks:

1. `s3_ingestion` — reads the raw CSVs from the S3-backed Unity Catalog
   volume and lands them as Bronze Delta tables.
2. `retail_pipeline` — runs the Silver cleaning/validation logic and the
   Gold aggregations on top of Bronze.

## Tech stack

| Layer | Tool |
|---|---|
| Storage (raw) | AWS S3, accessed via Databricks Unity Catalog |
| Processing | PySpark, Databricks, Delta Lake |
| Orchestration | Databricks Jobs |
| Analytics warehouse | Google BigQuery |
| Language / tests | Python, SQL, pytest |

## Project structure

```
data/               Synthetic sample source datasets (customers, orders, products)
src/
  bronze.py         Bronze ingestion entry point
  silver.py         Silver cleaning/validation entry point
  gold.py           Gold aggregation entry point
  transformations.py   Reusable, environment-agnostic PySpark transformation
                        logic (pure DataFrame in -> DataFrame out functions),
                        shared by bronze/silver/gold locally and on Databricks
sql/
  analytics.sql     BigQuery analytics queries against the Gold tables
tests/
  test_data_quality.py   pytest/PySpark data-quality tests for the
                          transformation logic
  conftest.py       Shared local Spark test fixture
docs/
  architecture.md   Detailed architecture documentation
```

## Key design decisions

- **Reusable transformation logic.** All business rules (cleaning,
  validation, deduplication, foreign-key checks, and aggregation) live in
  `src/transformations.py` as pure functions that take Spark DataFrames
  in and return Spark DataFrames out, with no file paths or I/O. The same
  functions run unchanged whether they're called locally (against Parquet
  files, for development and testing) or from the Databricks Job (against
  Delta tables backed by S3).
- **Rejected records are never silently dropped.** Every Silver rule that
  fails a row routes it to a dedicated rejected-record table with a
  `validation_reason` column, so data quality issues are always visible
  and auditable.
- **Tested independently of any cloud environment.** The pytest suite
  builds small synthetic Spark DataFrames in memory and validates every
  cleaning rule and aggregation directly — it does not depend on AWS,
  Databricks, or BigQuery to run.

## Running the tests

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/test_data_quality.py -v
```

## Running the pipeline locally

The `src/bronze.py`, `src/silver.py`, and `src/gold.py` scripts can run
against local Parquet files for development, using the same
`transformations.py` logic that runs in the Databricks Job:

```bash
export MASTER=local[*]
python src/bronze.py
python src/silver.py
python src/gold.py
```

## Documentation

See [docs/architecture.md](docs/architecture.md) for a full breakdown of
the data flow, cloud components, and orchestration.

## Built with

This project was developed with the assistance of
[Claude Code](https://claude.com/claude-code), used for scaffolding the
project structure, refactoring shared transformation logic, debugging
the local Spark/Java environment, and generating the pytest test suite.
