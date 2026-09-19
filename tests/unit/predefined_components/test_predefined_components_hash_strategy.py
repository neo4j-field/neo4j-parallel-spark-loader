from typing import Dict, List

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

from neo4j_parallel_spark_loader.predefined_components import (
    group_and_batch_spark_dataframe,
)
from neo4j_parallel_spark_loader.predefined_components.grouping import (
    create_node_groupings,
)


@pytest.fixture(scope="module")
def predefined_components_hash_data() -> List[Dict[str, str]]:
    return [{"partition_col": f"component-{i % 53}"} for i in range(500)]


def test_create_node_groupings_hash_strategy_range(
    spark_fixture: SparkSession,
    predefined_components_hash_data: List[Dict[str, str]],
) -> None:
    sdf = spark_fixture.createDataFrame(predefined_components_hash_data)
    num_groups = 4

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        partition_col="partition_col",
        num_groups=num_groups,
        strategy="hash",
    )

    assert result.filter(col("group").isNull()).count() == 0
    assert result.filter((col("group") < 0) | (col("group") >= num_groups)).count() == 0


def test_group_and_batch_spark_dataframe_hash_strategy(
    spark_fixture: SparkSession,
    predefined_components_hash_data: List[Dict[str, str]],
) -> None:
    sdf = spark_fixture.createDataFrame(predefined_components_hash_data)
    num_groups = 4

    result: DataFrame = group_and_batch_spark_dataframe(
        sdf, "partition_col", num_groups, strategy="hash"
    )

    assert result.filter(col("group").isNull()).count() == 0
    assert result.filter(col("batch").isNull()).count() == 0

    # deadlock invariant: within a batch, each partition_col value maps to exactly one group
    max_groups_per_partition_per_batch = (
        result.groupBy("batch", "partition_col")
        .agg(countDistinct("group").alias("distinct_groups"))
        .agg({"distinct_groups": "max"})
        .collect()[0][0]
    )
    assert max_groups_per_partition_per_batch == 1


def test_group_and_batch_spark_dataframe_hash_strategy_deterministic(
    spark_fixture: SparkSession,
    predefined_components_hash_data: List[Dict[str, str]],
) -> None:
    sdf = spark_fixture.createDataFrame(predefined_components_hash_data)
    num_groups = 4

    first_result = (
        group_and_batch_spark_dataframe(
            sdf, "partition_col", num_groups, strategy="hash"
        )
        .orderBy("partition_col")
        .collect()
    )
    second_result = (
        group_and_batch_spark_dataframe(
            sdf, "partition_col", num_groups, strategy="hash"
        )
        .orderBy("partition_col")
        .collect()
    )

    assert first_result == second_result
