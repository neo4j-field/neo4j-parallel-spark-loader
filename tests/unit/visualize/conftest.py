import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="function")
def heatmap_spark_dataframe(spark_fixture: SparkSession) -> DataFrame:
    data = [
        {"group": 0, "batch": 0, "count": 10},
        {"group": 0, "batch": 1, "count": 12},
        {"group": 0, "batch": 2, "count": 8},
        {"group": 1, "batch": 0, "count": 20},
        {"group": 1, "batch": 1, "count": 5},
        {"group": 1, "batch": 2, "count": 24},
        {"group": 2, "batch": 0, "count": 12},
        {"group": 2, "batch": 1, "count": 46},
        {"group": 2, "batch": 2, "count": 3},
        {"group": 3, "batch": 0, "count": 17},
        {"group": 3, "batch": 1, "count": 5},
    ]
    return spark_fixture.createDataFrame(data=data)
