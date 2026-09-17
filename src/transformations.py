# transformations.py
#
# Purpose: Reusable PySpark DataFrame transformation logic for the Silver
# and Gold layers.
#
# This file contains ONLY pure transformation functions:
#   - they take Spark DataFrame(s) in
#   - they return Spark DataFrame(s) out
#   - they never read from or write to a file, table, or path
#   - they contain no local file paths (e.g. no "data/..." anywhere)
#
# Why this matters: bronze.py, silver.py and gold.py each build a
# SparkSession and know about local file paths (data/bronze, data/silver,
# data/gold). That part is specific to running on a laptop. This file has
# none of that, so the exact same functions can be imported and called
# from a Databricks notebook or job, which reads/writes via different
# paths (e.g. S3 or a Databricks table) - only the calling code around it
# changes, never the business logic itself.
#
# No validation rules or aggregation logic were changed while moving them
# here - this is a pure "cut and paste" refactor.

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Shared helper (used by all three Silver cleaning functions below).
# ---------------------------------------------------------------------------

def split_clean_and_rejected(df: DataFrame):
    """Split a DataFrame (that already has a validation_reason column)
    into (clean_rows, rejected_rows).

    A row with validation_reason = NULL passed every rule and is "clean".
    A row with validation_reason set failed at least one rule.
    """
    clean_df = df.filter(F.col("validation_reason").isNull()).drop("validation_reason")
    rejected_df = df.filter(F.col("validation_reason").isNotNull())
    return clean_df, rejected_df


# ---------------------------------------------------------------------------
# Silver: cleaning rules, one function per table.
# ---------------------------------------------------------------------------

def clean_customers(customers_df: DataFrame):
    """Apply the customers cleaning rules and return (clean_df, rejected_df)."""

    # Trim whitespace from text columns. This is a safe transformation -
    # it never changes which rows are valid, only how the text looks.
    df = customers_df
    for column in ["first_name", "last_name", "email", "city", "country"]:
        df = df.withColumn(column, F.trim(F.col(column)))

    # To find duplicate customer_id values, number the rows within each
    # customer_id group (1, 2, 3, ...). Row number 1 is kept; anything
    # after that is a duplicate. The ordering here is arbitrary (just
    # "first row encountered") - good enough for learning purposes.
    dedup_window = Window.partitionBy("customer_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_row_number", F.row_number().over(dedup_window))

    # Build one validation_reason column. F.when(...) checks conditions in
    # order and stops at the first match, so priority = order written here.
    df = df.withColumn(
        "validation_reason",
        F.when(F.col("customer_id").isNull(), F.lit("missing customer_id"))
         .when(F.col("email").isNull() | (F.col("email") == ""), F.lit("missing or blank email"))
         .when(F.col("_row_number") > 1, F.lit("duplicate customer_id"))
         .otherwise(F.lit(None))
    ).drop("_row_number")

    return split_clean_and_rejected(df)


def clean_products(products_df: DataFrame):
    """Apply the products cleaning rules and return (clean_df, rejected_df)."""

    df = products_df
    for column in ["product_name", "category"]:
        df = df.withColumn(column, F.trim(F.col(column)))

    dedup_window = Window.partitionBy("product_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_row_number", F.row_number().over(dedup_window))

    df = df.withColumn(
        "validation_reason",
        F.when(F.col("product_id").isNull(), F.lit("missing product_id"))
         .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), F.lit("unit_price must be greater than 0"))
         .when(F.col("_row_number") > 1, F.lit("duplicate product_id"))
         .otherwise(F.lit(None))
    ).drop("_row_number")

    return split_clean_and_rejected(df)


def clean_orders(orders_df: DataFrame, clean_customers_df: DataFrame, clean_products_df: DataFrame):
    """Apply the orders cleaning rules and return (clean_df, rejected_df).

    clean_customers_df / clean_products_df must already be cleaned - an
    order is only kept if it points to a customer/product that survived
    its own cleaning step. This is how we enforce the foreign key rules.
    """

    # Standardize order_status regardless of whether the row is valid,
    # so both the Silver table and the rejected table look consistent.
    df = orders_df.withColumn("order_status", F.upper(F.trim(F.col("order_status"))))

    # Number rows within each order_id group to find duplicates, same idea
    # as in clean_customers / clean_products.
    dedup_window = Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_row_number", F.row_number().over(dedup_window))

    # Get the list of valid IDs from the already-cleaned tables, and left
    # join them in. If an order's customer_id/product_id has no match,
    # the joined column comes back NULL - that's how we detect a broken
    # foreign key.
    valid_customer_ids = clean_customers_df.select("customer_id").distinct() \
        .withColumnRenamed("customer_id", "_valid_customer_id")
    valid_product_ids = clean_products_df.select("product_id").distinct() \
        .withColumnRenamed("product_id", "_valid_product_id")

    df = df.join(valid_customer_ids, df["customer_id"] == F.col("_valid_customer_id"), "left")
    df = df.join(valid_product_ids, df["product_id"] == F.col("_valid_product_id"), "left")

    df = df.withColumn(
        "validation_reason",
        F.when(F.col("order_id").isNull(), F.lit("missing order_id"))
         .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity must be greater than 0"))
         .when(F.col("_row_number") > 1, F.lit("duplicate order_id"))
         .when(F.col("customer_id").isNull(), F.lit("missing customer_id"))
         .when(F.col("product_id").isNull(), F.lit("missing product_id"))
         .when(F.col("_valid_customer_id").isNull(), F.lit("customer_id not found in cleaned customers"))
         .when(F.col("_valid_product_id").isNull(), F.lit("product_id not found in cleaned products"))
         .otherwise(F.lit(None))
    ).drop("_row_number", "_valid_customer_id", "_valid_product_id")

    return split_clean_and_rejected(df)


# ---------------------------------------------------------------------------
# Gold: join + aggregate.
# ---------------------------------------------------------------------------

def build_enriched_orders(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """Join orders with customers and products, and add line_total.

    Joining on="customer_id" (a column name, not a condition like
    a.customer_id == b.customer_id) tells Spark to merge that column
    instead of keeping two separate copies of it - this avoids
    "ambiguous column" errors later when we just say "customer_id".
    """
    enriched_df = (
        orders_df
        .join(customers_df, on="customer_id", how="inner")
        .join(products_df, on="product_id", how="inner")
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    )
    return enriched_df


def build_product_performance(enriched_df: DataFrame) -> DataFrame:
    """One row per product: how much of it sold, and for how much money."""
    return enriched_df.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("line_total").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
    )


def build_category_performance(enriched_df: DataFrame) -> DataFrame:
    """One row per category: same measures as product_performance, but
    grouped one level higher (category instead of individual product)."""
    return enriched_df.groupBy("category").agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("line_total").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
    )


def build_customer_summary(enriched_df: DataFrame) -> DataFrame:
    """One row per customer: how many orders they placed and how much
    they spent in total."""
    return enriched_df.groupBy("customer_id", "first_name", "last_name").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_purchased"),
        F.sum("line_total").alias("total_spend"),
    )


def build_overall_metrics(enriched_df: DataFrame) -> DataFrame:
    """A single-row summary of the whole business: total orders, total
    revenue, and the average amount spent per order."""
    return enriched_df.agg(
        F.count("order_id").alias("total_orders"),
        F.sum("line_total").alias("total_revenue"),
    ).withColumn(
        "average_order_value",
        F.col("total_revenue") / F.col("total_orders"),
    )
