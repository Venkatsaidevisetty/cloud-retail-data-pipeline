# Architecture

This document will describe the architecture of the Cloud Retail Data
Engineering Pipeline.

## Planned overview

- **Source data**: `customers.csv`, `orders.csv`, `products.csv`
- **Ingestion**: AWS S3 (raw storage) + Databricks (processing)
- **Processing layers (medallion architecture)**:
  - Bronze — raw ingested data (`src/bronze.py`)
  - Silver — cleaned, validated data (`src/silver.py`)
  - Gold — business-ready aggregated data (`src/gold.py`)
- **Analytics**: Google BigQuery + SQL (`sql/analytics.sql`)
- **Data quality**: automated tests (`tests/test_data_quality.py`)

## Status

This is a starter/skeleton project. No pipeline logic has been built yet.
This document will be expanded as each layer is implemented.
