from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

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
