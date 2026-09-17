# silver.py
#
# Purpose: Silver layer - take the raw Bronze data and turn it into
# clean, trustworthy data.
#
# What "cleaning" means here:
#   - fixing formatting issues (extra whitespace, inconsistent casing)
#   - removing rows that are duplicates or that break basic rules
#     (missing IDs, invalid numbers, broken relationships between tables)
#
# Rows that fail a rule are NOT silently dropped - they are written out
# to separate "rejected_*" folders with a validation_reason column, so
# you can always see exactly what was removed and why.

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from transformations import clean_customers, clean_products, clean_orders

# ---------------------------------------------------------------------------
# 1. Paths - same pattern as bronze.py, so this script also works no matter
#    which folder you run it from.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"


def create_spark_session() -> SparkSession:
    """Create (or reuse) a SparkSession for this job.

    No .master(...) is set here on purpose: on Databricks, the cluster
    already provides a master, and setting one in code would override it.
    When running locally, set the MASTER environment variable instead,
    e.g. `export MASTER=local[*]` before running this script.
    """
    return (
        SparkSession.builder
        .appName("SilverLayer")
        .getOrCreate()
    )


def read_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Bronze Parquet table by name, e.g. 'customers'."""
    input_path = BRONZE_DIR / table_name
    return spark.read.parquet(str(input_path))


def write_silver(df: DataFrame, table_name: str) -> None:
    """Write a cleaned DataFrame to data/silver/<table_name>."""
    output_path = SILVER_DIR / table_name
    df.write.mode("overwrite").parquet(str(output_path))


def write_rejected(df: DataFrame, table_name: str) -> None:
    """Write rejected rows to data/silver/rejected_<table_name>."""
    output_path = SILVER_DIR / f"rejected_{table_name}"
    df.write.mode("overwrite").parquet(str(output_path))


def main():
    spark = create_spark_session()

    # Read the Bronze tables produced by bronze.py.
    customers_bronze = read_bronze_table(spark, "customers")
    products_bronze = read_bronze_table(spark, "products")
    orders_bronze = read_bronze_table(spark, "orders")

    # Clean customers and products first - orders depends on both of them
    # for its foreign key checks.
    customers_clean, customers_rejected = clean_customers(customers_bronze)
    products_clean, products_rejected = clean_products(products_bronze)
    orders_clean, orders_rejected = clean_orders(orders_bronze, customers_clean, products_clean)

    # Write the clean Silver tables.
    write_silver(customers_clean, "customers")
    write_silver(products_clean, "products")
    write_silver(orders_clean, "orders")

    # Write the rejected rows so nothing is lost silently.
    write_rejected(customers_rejected, "customers")
    write_rejected(products_rejected, "products")
    write_rejected(orders_rejected, "orders")

    print("Silver layer complete.")
    print(f"  customers: {customers_clean.count()} clean, {customers_rejected.count()} rejected")
    print(f"  products:  {products_clean.count()} clean, {products_rejected.count()} rejected")
    print(f"  orders:    {orders_clean.count()} clean, {orders_rejected.count()} rejected")

    spark.stop()


if __name__ == "__main__":
    main()
