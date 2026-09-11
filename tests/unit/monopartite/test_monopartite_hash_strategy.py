from typing import Dict, List

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

from neo4j_parallel_spark_loader.monopartite import group_and_batch_spark_dataframe
from neo4j_parallel_spark_loader.monopartite.grouping import create_node_groupings


@pytest.fixture(scope="module")
def monopartite_hash_data() -> List[Dict[str, int]]:
    return [
        {"source_node": i % 41, "target_node": (i * 3 + 5) % 41} for i in range(500)
    ]


def test_create_node_groupings_hash_strategy_range(
    spark_fixture: SparkSession, monopartite_hash_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(monopartite_hash_data)
    num_groups = 6

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        source_col="source_node",
        target_col="target_node",
        num_groups=num_groups,
        strategy="hash",
    )

    for col_name in ["source_group", "target_group", "group"]:
        assert result.filter(col(col_name).isNull()).count() == 0

    assert (
        result.filter(
            (col("source_group") < 0) | (col("source_group") >= num_groups)
        ).count()
        == 0
    )
    assert (
        result.filter(
            (col("target_group") < 0) | (col("target_group") >= num_groups)
        ).count()
        == 0
    )


def test_create_node_groupings_hash_strategy_consistent_source_and_target(
    spark_fixture: SparkSession, monopartite_hash_data: List[Dict[str, int]]
) -> None:
    """The same node ID must map to the same group whether it appears as a source or a target."""
    sdf = spark_fixture.createDataFrame(monopartite_hash_data)
    num_groups = 6

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        source_col="source_node",
        target_col="target_node",
        num_groups=num_groups,
        strategy="hash",
    )

    source_groups = result.select(
        col("source_node").alias("node"), col("source_group").alias("g")
    ).distinct()
    target_groups = result.select(
        col("target_node").alias("node"), col("target_group").alias("g")
    ).distinct()

    combined = source_groups.union(target_groups).distinct()

    max_groups_per_node = (
        combined.groupBy("node")
        .agg(countDistinct("g").alias("distinct_groups"))
        .agg({"distinct_groups": "max"})
        .collect()[0][0]
    )
    assert max_groups_per_node == 1


def test_group_and_batch_spark_dataframe_hash_strategy(
    spark_fixture: SparkSession, monopartite_hash_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(monopartite_hash_data)
    num_groups = 6

    result: DataFrame = group_and_batch_spark_dataframe(
        sdf, "source_node", "target_node", num_groups, strategy="hash"
    )

    assert result.filter(col("group").isNull()).count() == 0
    assert result.filter(col("batch").isNull()).count() == 0

    # deadlock invariant: within a batch, each node (as source or target) maps to one group
    sources = result.select("batch", "group", col("source_node").alias("node"))
    targets = result.select("batch", "group", col("target_node").alias("node"))
    combined = sources.unionAll(targets)

    max_groups_per_node_per_batch = (
        combined.groupBy("batch", "node")
        .agg(countDistinct("group").alias("distinct_groups"))
        .agg({"distinct_groups": "max"})
        .collect()[0][0]
    )
    assert max_groups_per_node_per_batch == 1


def test_group_and_batch_spark_dataframe_hash_strategy_deterministic(
    spark_fixture: SparkSession, monopartite_hash_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(monopartite_hash_data)
    num_groups = 6

    first_result = (
        group_and_batch_spark_dataframe(
            sdf, "source_node", "target_node", num_groups, strategy="hash"
        )
        .orderBy("source_node", "target_node")
        .collect()
    )
    second_result = (
        group_and_batch_spark_dataframe(
            sdf, "source_node", "target_node", num_groups, strategy="hash"
        )
        .orderBy("source_node", "target_node")
        .collect()
    )

    assert first_result == second_result


@pytest.mark.parametrize("strategy", ["greedy", "hash"])
def test_null_node_id_yields_null_group_and_batch(
    spark_fixture: SparkSession, strategy: str
) -> None:
    """
    `least`/`greatest` skip nulls, so a row with one null endpoint used to land in a real
    self-loop group (e.g. "3 -- 3") and a real batch. It must get a null group and batch.
    """
    sdf = spark_fixture.createDataFrame(
        [(1, 6), (2, 7), (None, 8), (3, None), (4, 4)],
        "source_node int, target_node int",
    )

    result = group_and_batch_spark_dataframe(
        sdf, "source_node", "target_node", 4, strategy=strategy
    ).collect()

    for row in result:
        has_null_id = row["source_node"] is None or row["target_node"] is None
        assert (row["group"] is None) == has_null_id
        assert (row["batch"] is None) == has_null_id
