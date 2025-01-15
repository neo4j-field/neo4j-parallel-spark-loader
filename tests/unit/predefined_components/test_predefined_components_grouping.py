from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

from neo4j_parallel_spark_loader.predefined_components.grouping import (
    create_node_groupings,
)


def test_create_node_groupings(
    spark_fixture: SparkSession,
    predefined_components_grouping_data: List[Dict[str, int]],
) -> None:
    sdf = spark_fixture.createDataFrame(predefined_components_grouping_data)

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        partition_col="partition_col",
        num_groups=4,
    )

    group_count = result.select(countDistinct("group")).collect()[0][0]

    assert group_count == 2
    assert "group" in result.columns
    assert result.filter(col("group").isNull()).count() == 0


def test_create_node_groups_at_scale_partition_values_present_in_only_single_group_each(
    reddit_threads_predefined_components_spark_dataframe: DataFrame,
) -> None:
    processed_sdf = create_node_groupings(
        spark_dataframe=reddit_threads_predefined_components_spark_dataframe,
        partition_col="partition_col",
        num_groups=5,
    )

    # test partition values present in only single group each
    part_groups = (
        processed_sdf.select("partition_col", "group")
        .groupBy("partition_col")
        .count()
        .orderBy("count")
    )

    assert part_groups.select("count").first()[0] < 2


def test_create_node_groups_at_scale_key_values_present_in_only_single_group_each(
    reddit_threads_predefined_components_spark_dataframe: DataFrame,
) -> None:
    processed_sdf = create_node_groupings(
        spark_dataframe=reddit_threads_predefined_components_spark_dataframe,
        partition_col="partition_col",
        num_groups=5,
    )

    # test key values present in only single group each
    key_groups = (
        processed_sdf.select("source", "group")
        .withColumnRenamed("source", "key")
        .union(
            processed_sdf.select("target", "group").withColumnRenamed("target", "key")
        )
    )

    key_groups_count = key_groups.groupBy("key").count().orderBy("count")

    assert key_groups_count.select("count").first()[0] < 2
