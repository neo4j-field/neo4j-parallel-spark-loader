import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def v2_spark():
    """Grouping needs local Spark, but no Neo4j connector or database."""
    existing = SparkSession.getActiveSession()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("v2 unit tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    previous = {
        key: spark.conf.get(key)
        for key in ("spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled")
    }
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    yield spark
    spark.catalog.clearCache()
    if existing is None:
        spark.stop()
    else:
        for key, value in previous.items():
            spark.conf.set(key, value)
