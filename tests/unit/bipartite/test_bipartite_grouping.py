from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

from parallel_spark_loader.bipartite.grouping import (
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

    assert "final_group" in result.columns

    for col_name in ["source_group", "target_group", "final_group"]:
        assert result.filter(col(col_name).isNull()).count() == 0
