import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_fixture():
    spark = (
        SparkSession.builder.appName("Unit and Integration Testing")
        .config(
            "spark.jars.packages",
            "org.neo4j:neo4j-connector-apache-spark_2.12:5.1.0_for_spark_3",
        )
        .config("neo4j.url", "neo4j://localhost:7687")
        .config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .getOrCreate()
    )
    yield spark


@pytest.fixture
def nondeterministic_id():
    """
    Return a function building a string column whose values change on every evaluation.

    Spark fixes the seed of built-in random expressions such as `rand()` and `uuid()` when the
    plan is analyzed, so re-evaluating the same DataFrame reproduces their values. A Python UDF
    marked non-deterministic does not, which mimics an input whose upstream lineage yields
    different rows each time it is recomputed.
    """
    import random

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    return udf(
        lambda: f"id-{random.randrange(10**12)}", StringType()
    ).asNondeterministic()
