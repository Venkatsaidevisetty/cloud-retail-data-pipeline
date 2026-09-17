# gold.py
#
# Purpose: Gold layer - turn the cleaned Silver data into business-ready,
# aggregated tables that analysts/BI tools would query directly.
#
# Nothing here re-checks data quality - that already happened in
# silver.py. This layer only joins and summarizes already-trustworthy data.

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from transformations import (
    build_enriched_orders,
    build_product_performance,
    build_category_performance,
    build_customer_summary,
    build_overall_metrics,
)

# ---------------------------------------------------------------------------
# 1. Paths - same pattern as bronze.py and silver.py.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"


def create_spark_session() -> SparkSession:
    """Create (or reuse) a SparkSession for this job.

    No .master(...) is set here on purpose: on Databricks, the cluster
    already provides a master, and setting one in code would override it.
    When running locally, set the MASTER environment variable instead,
    e.g. `export MASTER=local[*]` before running this script.
    """
    return (
        SparkSession.builder
        .appName("GoldLayer")
        .getOrCreate()
    )


def read_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a clean Silver Parquet table by name, e.g. 'customers'."""
    input_path = SILVER_DIR / table_name
    return spark.read.parquet(str(input_path))


def write_gold(df: DataFrame, table_name: str) -> None:
    """Write a Gold DataFrame to data/gold/<table_name>."""
    output_path = GOLD_DIR / table_name
    df.write.mode("overwrite").parquet(str(output_path))


def main():
    spark = create_spark_session()

    # Read the clean tables produced by silver.py.
    customers_df = read_silver_table(spark, "customers")
    products_df = read_silver_table(spark, "products")
    orders_df = read_silver_table(spark, "orders")

    # Build the one shared, joined table that every Gold table is derived from.
    enriched_df = build_enriched_orders(orders_df, customers_df, products_df)

    # Build each Gold table.
    product_performance_df = build_product_performance(enriched_df)
    category_performance_df = build_category_performance(enriched_df)
    customer_summary_df = build_customer_summary(enriched_df)
    overall_metrics_df = build_overall_metrics(enriched_df)

    # Write each one out as Parquet under data/gold/.
    write_gold(product_performance_df, "product_performance")
    write_gold(category_performance_df, "category_performance")
    write_gold(customer_summary_df, "customer_summary")
    write_gold(overall_metrics_df, "overall_metrics")

    print("Gold layer complete. Row counts:")
    print(f"  product_performance:  {product_performance_df.count()}")
    print(f"  category_performance: {category_performance_df.count()}")
    print(f"  customer_summary:     {customer_summary_df.count()}")
    print(f"  overall_metrics:      {overall_metrics_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
