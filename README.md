# Cloud Retail Data Engineering Pipeline

A learning project for building a retail data pipeline using Python,
PySpark, Databricks, AWS S3, BigQuery, and SQL.

## Project structure

```
data/     Sample source datasets (customers, orders, products)
src/      Pipeline code (bronze/silver/gold layers)
sql/      SQL queries for analytics (e.g. BigQuery)
tests/    Data quality tests
docs/     Project documentation (architecture, notes)
```

## Status

This is a starter skeleton. The datasets are small, synthetic samples,
and the Python/SQL files currently contain only placeholder comments
describing what they will do. No pipeline logic has been implemented yet.

## Planned architecture

Raw CSVs -> Bronze (raw ingest) -> Silver (cleaned) -> Gold (aggregated)
-> BigQuery (analytics via SQL)

See [docs/architecture.md](docs/architecture.md) for more detail.

## Setup (for later, once code is implemented)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
