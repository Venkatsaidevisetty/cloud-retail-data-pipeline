# test_data_quality.py
#
# Purpose: automated tests for the reusable transformation functions in
# src/transformations.py.
#
# These tests build tiny, synthetic Spark DataFrames by hand (a handful
# of rows each) and check that clean_customers / clean_products /
# clean_orders / build_enriched_orders / build_product_performance behave
# the way the business rules say they should. Nothing here reads a real
# file or talks to AWS, Databricks, S3, or BigQuery - everything runs
# against small in-memory DataFrames using a local Spark session (see
# the `spark` fixture in conftest.py).
#
# How to run these tests:
#   .venv/bin/pytest tests/test_data_quality.py -v

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from transformations import (
    clean_customers,
    clean_products,
    clean_orders,
    build_enriched_orders,
    build_product_performance,
)

# ---------------------------------------------------------------------------
# Schemas + small helper functions to build test DataFrames.
#
# We use explicit schemas (instead of letting Spark guess) so that a
# missing value can be represented as a real Python None and Spark treats
# it as a proper SQL NULL - the same as clean_customers/clean_products/
# clean_orders expect from real Bronze data.
# ---------------------------------------------------------------------------

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("unit_price", DoubleType()),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("order_status", StringType()),
])


def make_customers_df(spark, rows):
    return spark.createDataFrame(rows, schema=CUSTOMERS_SCHEMA)


def make_products_df(spark, rows):
    return spark.createDataFrame(rows, schema=PRODUCTS_SCHEMA)


def make_orders_df(spark, rows):
    return spark.createDataFrame(rows, schema=ORDERS_SCHEMA)


def reasons_in(df):
    """Collect the validation_reason values from a rejected_df as a set,
    so a test can check "this reason showed up" without caring about
    row order."""
    return {row.validation_reason for row in df.select("validation_reason").collect()}


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------

def test_duplicate_customer_id_is_rejected(spark):
    """Two rows sharing the same customer_id: one should stay clean,
    the other should be rejected with reason 'duplicate customer_id'."""
    customers_df = make_customers_df(spark, [
        (1, "Asha", "Rao", "asha@example.com", "Bengaluru", "India"),
        (1, "Asha", "Rao", "asha.dup@example.com", "Bengaluru", "India"),
    ])

    clean_df, rejected_df = clean_customers(customers_df)

    assert clean_df.count() == 1
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"duplicate customer_id"}


def test_blank_customer_email_is_rejected(spark):
    """A customer with an empty-string email should be rejected with
    reason 'missing or blank email', even though customer_id is fine."""
    customers_df = make_customers_df(spark, [
        (1, "Noah", "Lee", "", "Seoul", "South Korea"),
    ])

    clean_df, rejected_df = clean_customers(customers_df)

    assert clean_df.count() == 0
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"missing or blank email"}


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

def test_non_positive_unit_price_is_rejected(spark):
    """A product priced at 0 (or less) should be rejected - a real
    product must cost something."""
    products_df = make_products_df(spark, [
        (101, "Broken Item", "Misc", 0.0),
    ])

    clean_df, rejected_df = clean_products(products_df)

    assert clean_df.count() == 0
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"unit_price must be greater than 0"}


# ---------------------------------------------------------------------------
# Orders
#
# clean_orders() needs already-cleaned customers/products DataFrames to
# check foreign keys against, so each orders test builds a small "known
# good" set of clean customers/products first.
# ---------------------------------------------------------------------------

def _sample_clean_customers(spark):
    customers_df = make_customers_df(spark, [
        (1, "Asha", "Rao", "asha@example.com", "Bengaluru", "India"),
        (2, "Liam", "Smith", "liam@example.com", "Toronto", "Canada"),
    ])
    clean_df, _ = clean_customers(customers_df)
    return clean_df


def _sample_clean_products(spark):
    products_df = make_products_df(spark, [
        (101, "Wireless Mouse", "Electronics", 19.99),
        (102, "Running Shoes", "Footwear", 49.99),
    ])
    clean_df, _ = clean_products(products_df)
    return clean_df


def test_duplicate_order_id_is_rejected(spark):
    """Two rows sharing the same order_id: the second is a duplicate."""
    clean_customers_df = _sample_clean_customers(spark)
    clean_products_df = _sample_clean_products(spark)

    orders_df = make_orders_df(spark, [
        (1001, 1, 101, 2, "completed"),
        (1001, 2, 102, 1, "completed"),
    ])

    clean_df, rejected_df = clean_orders(orders_df, clean_customers_df, clean_products_df)

    assert clean_df.count() == 1
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"duplicate order_id"}


def test_non_positive_quantity_is_rejected(spark):
    """An order for 0 units doesn't make sense and should be rejected."""
    clean_customers_df = _sample_clean_customers(spark)
    clean_products_df = _sample_clean_products(spark)

    orders_df = make_orders_df(spark, [
        (1001, 1, 101, 0, "pending"),
    ])

    clean_df, rejected_df = clean_orders(orders_df, clean_customers_df, clean_products_df)

    assert clean_df.count() == 0
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"quantity must be greater than 0"}


def test_unknown_customer_id_is_rejected(spark):
    """An order pointing at a customer_id that doesn't exist in the
    cleaned customers table should be rejected - this is the foreign
    key check."""
    clean_customers_df = _sample_clean_customers(spark)
    clean_products_df = _sample_clean_products(spark)

    orders_df = make_orders_df(spark, [
        (1001, 999, 101, 1, "completed"),  # customer_id 999 does not exist
    ])

    clean_df, rejected_df = clean_orders(orders_df, clean_customers_df, clean_products_df)

    assert clean_df.count() == 0
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"customer_id not found in cleaned customers"}


def test_unknown_product_id_is_rejected(spark):
    """Same idea as above, but for product_id."""
    clean_customers_df = _sample_clean_customers(spark)
    clean_products_df = _sample_clean_products(spark)

    orders_df = make_orders_df(spark, [
        (1001, 1, 999, 1, "completed"),  # product_id 999 does not exist
    ])

    clean_df, rejected_df = clean_orders(orders_df, clean_customers_df, clean_products_df)

    assert clean_df.count() == 0
    assert rejected_df.count() == 1
    assert reasons_in(rejected_df) == {"product_id not found in cleaned products"}


def test_valid_rows_remain_clean(spark):
    """A fully valid set of customers/products/orders should pass
    straight through every rule with nothing rejected."""
    customers_df = make_customers_df(spark, [
        (1, "Asha", "Rao", "asha@example.com", "Bengaluru", "India"),
        (2, "Liam", "Smith", "liam@example.com", "Toronto", "Canada"),
    ])
    products_df = make_products_df(spark, [
        (101, "Wireless Mouse", "Electronics", 19.99),
        (102, "Running Shoes", "Footwear", 49.99),
    ])
    orders_df = make_orders_df(spark, [
        (1001, 1, 101, 2, "completed"),
        (1002, 2, 102, 1, "pending"),
    ])

    clean_customers_df, rejected_customers_df = clean_customers(customers_df)
    clean_products_df, rejected_products_df = clean_products(products_df)
    clean_orders_df, rejected_orders_df = clean_orders(orders_df, clean_customers_df, clean_products_df)

    assert clean_customers_df.count() == 2
    assert clean_products_df.count() == 2
    assert clean_orders_df.count() == 2
    assert rejected_customers_df.count() == 0
    assert rejected_products_df.count() == 0
    assert rejected_orders_df.count() == 0

    # order_status should be standardized to upper case even for clean rows.
    statuses = {row.order_status for row in clean_orders_df.collect()}
    assert statuses == {"COMPLETED", "PENDING"}


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

def test_build_enriched_orders_calculates_line_total(spark):
    """line_total should always equal quantity * unit_price for each order."""
    customers_df = make_customers_df(spark, [
        (1, "Asha", "Rao", "asha@example.com", "Bengaluru", "India"),
    ])
    products_df = make_products_df(spark, [
        (101, "Wireless Mouse", "Electronics", 20.0),
    ])
    orders_df = make_orders_df(spark, [
        (1001, 1, 101, 3, "completed"),  # 3 units * $20.00 = $60.00
    ])

    enriched_df = build_enriched_orders(orders_df, customers_df, products_df)
    row = enriched_df.collect()[0]

    assert row.quantity == 3
    assert row.unit_price == 20.0
    assert row.line_total == 60.0


def test_product_performance_totals_are_correct(spark):
    """Two separate orders for the same product should be summed
    together into one product_performance row."""
    customers_df = make_customers_df(spark, [
        (1, "Asha", "Rao", "asha@example.com", "Bengaluru", "India"),
        (2, "Liam", "Smith", "liam@example.com", "Toronto", "Canada"),
    ])
    products_df = make_products_df(spark, [
        (101, "Wireless Mouse", "Electronics", 20.0),
    ])
    orders_df = make_orders_df(spark, [
        (1001, 1, 101, 2, "completed"),  # 2 * $20.00 = $40.00
        (1002, 2, 101, 3, "completed"),  # 3 * $20.00 = $60.00
    ])

    enriched_df = build_enriched_orders(orders_df, customers_df, products_df)
    performance_df = build_product_performance(enriched_df)
    row = performance_df.collect()[0]

    assert row.product_id == 101
    assert row.total_units_sold == 5      # 2 + 3
    assert row.total_revenue == 100.0     # 40.00 + 60.00
    assert row.total_orders == 2          # two separate order rows
