# bronze.py
#
# Purpose: Bronze layer - ingest the raw CSV files with as little change
# as possible, and save them in a more efficient format (Parquet) so
# later steps (Silver, Gold) have a consistent starting point.
#
# Rule of thumb for Bronze: don't clean, don't join, don't filter.
# Just load the data faithfully and store it.

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

# ---------------------------------------------------------------------------
# 1. Work out file paths relative to this script, so it runs the same way
#    no matter which folder you launch it from.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"

CUSTOMERS_CSV = DATA_DIR / "customers.csv"
ORDERS_CSV = DATA_DIR / "orders.csv"
PRODUCTS_CSV = DATA_DIR / "products.csv"

# ---------------------------------------------------------------------------
# 2. Define an explicit schema for each file.
#    We do this instead of letting Spark "guess" (inferSchema=True) so that
#    column types are predictable and don't silently change between runs.
#    This still counts as "raw" data - we are only describing the types
#    that are already there, not changing any values.
# ---------------------------------------------------------------------------
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("product_id", IntegerType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
])


def create_spark_session() -> SparkSession:
    """Create (or reuse) a SparkSession for this job.

    No .master(...) is set here on purpose: on Databricks, the cluster
    already provides a master, and setting one in code would override it.
    When running locally, set the MASTER environment variable instead,
    e.g. `export MASTER=local[*]` before running this script.
    """
    return (
        SparkSession.builder
        .appName("BronzeLayer")
        .getOrCreate()
    )


def ingest_csv(spark: SparkSession, csv_path: Path, schema: StructType):
    """Read one raw CSV file into a Spark DataFrame using a fixed schema."""
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(str(csv_path))
    )


def write_bronze(df, table_name: str):
    """Write a DataFrame to the Bronze folder as Parquet.

    'overwrite' mode means re-running this script replaces the old
    Bronze output instead of duplicating it.
    """
    output_path = BRONZE_DIR / table_name
    df.write.mode("overwrite").parquet(str(output_path))


def main():
    spark = create_spark_session()

    # Read each raw file with its schema.
    customers_df = ingest_csv(spark, CUSTOMERS_CSV, CUSTOMERS_SCHEMA)
    products_df = ingest_csv(spark, PRODUCTS_CSV, PRODUCTS_SCHEMA)
    orders_df = ingest_csv(spark, ORDERS_CSV, ORDERS_SCHEMA)

    # Write each one out as a Bronze table, unchanged in content.
    write_bronze(customers_df, "customers")
    write_bronze(products_df, "products")
    write_bronze(orders_df, "orders")

    print("Bronze layer complete. Row counts:")
    print(f"  customers: {customers_df.count()}")
    print(f"  products:  {products_df.count()}")
    print(f"  orders:    {orders_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
