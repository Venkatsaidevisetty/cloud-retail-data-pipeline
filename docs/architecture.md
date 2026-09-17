# Architecture

This document describes the architecture of the Cloud Retail Data
Engineering Pipeline.

> **Data notice:** All data referenced by this pipeline (customers,
> products, orders) is **synthetic sample data**, generated for
> demonstration and testing. No real customer or business data is used
> anywhere in this project.

## Status: Completed

## Data flow

```
AWS S3 (raw retail CSVs: customers, orders, products)
   │
   │  Databricks Unity Catalog external location / volume
   │  provides governed, credential-managed access to the S3 bucket
   ▼
Bronze layer (Delta tables)
   - raw ingestion, minimal transformation
   - explicit schemas applied on read
   ▼
Silver layer (Delta tables + rejected-record tables)
   - trims/standardizes text fields
   - removes duplicate primary keys (customers, products, orders)
   - enforces required fields (e.g. non-null IDs, non-blank email)
   - enforces value rules (e.g. unit_price > 0, quantity > 0)
   - enforces referential integrity: an order is only kept if its
     customer_id and product_id both exist in the cleaned Silver
     customers/products tables
   - every rejected row is written to a rejected_* table with a
     validation_reason column - nothing is silently dropped
   ▼
Gold layer (Delta tables)
   - product_performance   — units sold, revenue, order count per product
   - category_performance  — the same measures rolled up by category
   - customer_summary      — orders, units purchased, and spend per customer
   - overall_metrics       — total orders, total revenue, average order value
   ▼
Google BigQuery
   - Gold tables are made available as BigQuery analytics tables
   - sql/analytics.sql contains the analytics queries run against them
```

## Orchestration

The pipeline runs as a **Databricks Job** with two tasks:

1. **`s3_ingestion`** — reads the raw CSV files from the S3-backed Unity
   Catalog volume and writes them as Bronze Delta tables.
2. **`retail_pipeline`** — depends on `s3_ingestion` and runs the Silver
   cleaning/validation logic followed by the Gold aggregation logic.

## Code organization

The business logic (cleaning rules, validation, deduplication,
referential-integrity checks, and aggregations) lives in
`src/transformations.py` as a set of pure functions:

- Each function takes Spark DataFrame(s) as input and returns Spark
  DataFrame(s) as output.
- None of these functions read from or write to a file, table, or path,
  and none contain environment-specific paths.
- `src/bronze.py`, `src/silver.py`, and `src/gold.py` are thin
  environment-specific wrappers: they handle reading input, calling the
  shared transformation functions, and writing output. Locally, that
  means reading/writing Parquet files on disk; on Databricks, the same
  functions are called against Delta tables sourced from S3.

This separation is what allows the same transformation logic to be
developed and unit-tested locally, and then run unchanged inside the
Databricks Job.

## Testing

`tests/test_data_quality.py` (with shared setup in `tests/conftest.py`)
contains pytest/PySpark tests that build small synthetic DataFrames
in-memory and verify the transformation functions directly - covering
duplicate detection, missing/invalid field rejection, foreign-key
enforcement, and Gold aggregation math. These tests run entirely on a
local Spark session and do not depend on AWS, Databricks, or BigQuery.

## Tooling

This project was built with the assistance of
[Claude Code](https://claude.com/claude-code) for project scaffolding,
refactoring the shared transformation logic out of the layer scripts,
debugging the local Spark/Java environment, and generating the
pytest data-quality test suite.
