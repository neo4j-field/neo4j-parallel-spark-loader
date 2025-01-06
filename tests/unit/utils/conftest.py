from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def utils_data() -> List[Dict[str, int]]:
    return [
        {"source_node": 1, "target_node": 6, "partition_col": "a"},
        {"source_node": 1, "target_node": 7, "partition_col": "a"},
        {"source_node": 1, "target_node": 8, "partition_col": "a"},
        {"source_node": 0, "target_node": 9, "partition_col": "b"},
        {"source_node": 0, "target_node": 0, "partition_col": "b"},
    ]


@pytest.fixture(scope="function")
def utils_complete_spark_dataframe(spark_fixture: SparkSession) -> DataFrame:
    data = [
        {
            "source_node": 1,
            "target_node": 6,
            "partition_col": "a",
            "final_group": 0,
            "batch": 0,
        },
        {
            "source_node": 1,
            "target_node": 7,
            "partition_col": "a",
            "final_group": 1,
            "batch": 0,
        },
        {
            "source_node": 1,
            "target_node": 8,
            "partition_col": "a",
            "final_group": 1,
            "batch": 1,
        },
        {
            "source_node": 0,
            "target_node": 9,
            "partition_col": "b",
            "final_group": 0,
            "batch": 1,
        },
        {
            "source_node": 0,
            "target_node": 0,
            "partition_col": "b",
            "final_group": 0,
            "batch": 2,
        },
    ]
    return spark_fixture.createDataFrame(data)


@pytest.fixture(scope="function")
def utils__spark_dataframe_missing_batch_column(
    spark_fixture: SparkSession,
) -> DataFrame:
    data = [
        {"source_node": 1, "target_node": 6, "partition_col": "a", "final_group": 0},
        {"source_node": 1, "target_node": 7, "partition_col": "a", "final_group": 1},
        {"source_node": 1, "target_node": 8, "partition_col": "a", "final_group": 1},
        {"source_node": 0, "target_node": 9, "partition_col": "b", "final_group": 0},
        {"source_node": 0, "target_node": 0, "partition_col": "b", "final_group": 0},
    ]
    return spark_fixture.createDataFrame(data)


@pytest.fixture(scope="function")
def mock_spark_session_incompatible() -> MagicMock:
    m = MagicMock(spec=SparkSession)

    m.version = "3.3.0"
    return m


@pytest.fixture(scope="function")
def mock_spark_session() -> MagicMock:
    m = MagicMock(spec=SparkSession)

    m.version = "3.5.0"
    return m
