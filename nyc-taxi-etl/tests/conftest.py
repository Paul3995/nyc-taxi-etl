"""
Shared pytest fixtures for the transformation test suite.

The SparkSession is session-scoped so Spark starts once and is reused across
all tests in the run.  Spark startup costs ~5–10 s; paying it once keeps the
test suite fast even as more tests are added.

spark.ui.enabled=false prevents Spark from binding a port during CI runs,
which avoids flaky "address already in use" failures.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("nyc-taxi-etl-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
