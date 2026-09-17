# conftest.py
#
# Purpose: shared pytest setup for all tests in this folder.
#
# pytest automatically finds and loads conftest.py - anything defined
# here (like the `spark` fixture below) is available to every test
# function without needing to import it.

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# The transformation functions we want to test live in src/transformations.py.
# That folder isn't installed as a Python package, so we add it to Python's
# import search path here. This lets test files do:
#   from transformations import clean_customers, ...
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(scope="session")
def spark():
    """Provide one shared local SparkSession for the whole test session.

    Starting Spark is slow, so scope="session" starts it once and reuses
    it across every test in this run, instead of once per test.
    This runs fully on your own machine (local[*]) - no AWS, Databricks,
    S3, or BigQuery involved.
    """
    spark_session = (
        SparkSession.builder
        .appName("DataQualityTests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
