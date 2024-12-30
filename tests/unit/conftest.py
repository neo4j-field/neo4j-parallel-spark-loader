import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Unit Testing").getOrCreate()
    yield spark
