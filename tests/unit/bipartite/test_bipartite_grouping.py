from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct, expr

from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe
from neo4j_parallel_spark_loader.bipartite.grouping import (
    create_node_groupings,
    create_value_counts_dataframe,
)


def test_create_value_counts_dataframe(
    spark_fixture: SparkSession, bipartite_grouping_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(bipartite_grouping_data)
    result: DataFrame = create_value_counts_dataframe(
        spark_dataframe=sdf, grouping_column="source_node"
    )

    result_dict = {}
    list_of_dicts = result.collect()
    [
        result_dict.update({row.asDict().get("source_node"): row.asDict().get("count")})
        for row in list_of_dicts
    ]

    assert result_dict.get(0) == 2
    assert result_dict.get(1) == 3


def test_create_node_groupings(
    spark_fixture: SparkSession, bipartite_grouping_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(bipartite_grouping_data)

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        source_col="source_node",
        target_col="target_node",
        num_groups=4,
    )

    source_group_count = result.select(countDistinct("source_group")).collect()[0][0]
    target_group_count = result.select(countDistinct("target_group")).collect()[0][0]

    assert source_group_count <= 4
    assert target_group_count <= 4

    assert "group" in result.columns

    for col_name in ["source_group", "target_group", "group"]:
        assert result.filter(col(col_name).isNull()).count() == 0


def test_greedy_groups_invalid_utf8_ids(spark_fixture: SparkSession) -> None:
    # 0xC3 followed by "(" is not valid UTF-8 and used to lose its group on the driver round trip
    id_expr = (
        "CASE WHEN id % 4 = 0 THEN cast(unhex('C328') AS STRING) "
        "ELSE cast(id % {m} AS STRING) END"
    )
    sdf = spark_fixture.range(20).select(
        expr(id_expr.format(m=3)).alias("source_node"),
        expr(id_expr.format(m=5)).alias("target_node"),
    )

    result = group_and_batch_spark_dataframe(
        sdf, "source_node", "target_node", num_groups=3
    )

    assert result.filter(col("group").isNull() | col("batch").isNull()).count() == 0


def test_greedy_groups_nondeterministic_input(
    spark_fixture: SparkSession, nondeterministic_id
) -> None:
    # every evaluation produces new ids, so none match the counting pass
    sdf = spark_fixture.range(50).select(
        nondeterministic_id().alias("source_node"),
        nondeterministic_id().alias("target_node"),
    )

    result = group_and_batch_spark_dataframe(
        sdf, "source_node", "target_node", num_groups=4
    )

    assert result.filter(col("group").isNull() | col("batch").isNull()).count() == 0


def test_greedy_null_id_yields_null_group_and_batch(
    spark_fixture: SparkSession,
) -> None:
    sdf = spark_fixture.createDataFrame(
        [(1, 6), (2, 7), (None, 8), (3, None)], "source_node long, target_node long"
    )

    result = group_and_batch_spark_dataframe(
        sdf, "source_node", "target_node", num_groups=2
    )

    null_rows = result.filter(
        col("source_node").isNull() | col("target_node").isNull()
    ).collect()
    assert len(null_rows) == 2
    assert all(row["group"] is None and row["batch"] is None for row in null_rows)
    assert result.filter(col("batch").isNull()).count() == 2
