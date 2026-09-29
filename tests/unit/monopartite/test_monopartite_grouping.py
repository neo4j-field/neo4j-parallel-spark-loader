from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct, expr

from neo4j_parallel_spark_loader.monopartite import group_and_batch_spark_dataframe
from neo4j_parallel_spark_loader.monopartite.grouping import (
    create_node_groupings,
    create_value_counts_dataframe,
)


def test_create_value_counts_dataframe(
    spark_fixture: SparkSession, monopartite_grouping_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(monopartite_grouping_data)
    result: DataFrame = create_value_counts_dataframe(
        spark_dataframe=sdf, source_col="source_node", target_col="target_node"
    )

    result_dict = {}
    list_of_dicts = result.collect()
    [
        result_dict.update(
            {row.asDict().get("combined_col"): row.asDict().get("count")}
        )
        for row in list_of_dicts
    ]

    assert result_dict.get(0) == 2
    assert result_dict.get(1) == 1
    assert result_dict.get(2) == 1
    assert result_dict.get(3) == 2
    assert result_dict.get(4) == 2
    assert result_dict.get(5) == 1
    assert result_dict.get(6) == 1


def test_create_node_groupings(
    spark_fixture: SparkSession, monopartite_grouping_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(monopartite_grouping_data)

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        source_col="source_node",
        target_col="target_node",
        num_groups=4,
    )

    source_group_count = result.select(countDistinct("source_group")).collect()[0][0]
    target_group_count = result.select(countDistinct("target_group")).collect()[0][0]

    combined_sdf = (
        result.withColumnsRenamed({"source_group": "combined_col"})
        .select("combined_col")
        .union(
            result.withColumnsRenamed({"target_group": "combined_col"}).select(
                "combined_col"
            )
        )
    )
    combined_group_count = combined_sdf.distinct().count()

    assert source_group_count <= 4
    assert target_group_count <= 4
    assert combined_group_count <= 4
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
        [(1, 6), (2, 7), (None, 8), (3, None), (4, 4)],
        "source_node long, target_node long",
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


def test_greedy_same_id_gets_same_group_across_mixed_types(
    spark_fixture: SparkSession,
) -> None:
    # int source and long target: xxhash64 depends on the type, so both are hashed as long
    sdf = spark_fixture.createDataFrame(
        [(i, i + 1) for i in range(20)], "source_node int, target_node long"
    )

    result = create_node_groupings(sdf, "source_node", "target_node", num_groups=4)

    source_groups = {
        row["source_node"]: row["source_group"]
        for row in result.select("source_node", "source_group").collect()
    }
    target_groups = {
        row["target_node"]: row["target_group"]
        for row in result.select("target_node", "target_group").collect()
    }
    shared_ids = source_groups.keys() & target_groups.keys()
    assert len(shared_ids) == 19
    assert all(source_groups[i] == target_groups[i] for i in shared_ids)
