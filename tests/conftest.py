import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_fixture():
    spark = (
        SparkSession.builder.appName("Unit and Integration Testing")
        .config('spark.jars.packages', 'org.neo4j:neo4j-connector-apache-spark_2.12:5.1.0_for_spark_3')
        .config("neo4j.url", "neo4j://localhost:7687")
        .config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", "neo4j")
        .config("neo4j.authentication.basic.password", "password")
        .getOrCreate()
    )
    yield spark
