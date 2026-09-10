from typing import Literal

from pyspark.sql import DataFrame

from ..utils.grouping import (
    create_group_column_from_source_and_target_groups,
    create_value_counts_dataframe,
    create_value_groupings,
)
from ..utils.hash_grouping import hash_group_column
from ..utils.verify_spark import verify_spark_version


def create_node_groupings(
    spark_dataframe: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    strategy: Literal["greedy", "hash"] = "greedy",
) -> DataFrame:
    """
    Create node groupings for parallel ingest into Neo4j.
    Add `source_group`, `target_group` and `group` columns to the Spark DataFrame identifying which groups the row belongs in.
    `group` is a concatenation of the source and target group values.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    source_col : str
        The column indicating the relationship source id.
    target_col : str
        The column indicating the relationship target id.
    num_groups : int
        The desired number of groups to generate. The process may generate less groups as necessary.
    strategy : Literal["greedy", "hash"], optional
        The grouping strategy to use. By default "greedy".
        "greedy" collects distinct source/target id counts to the driver and greedily
        bin-packs them into balanced groups. This scales poorly with the number of distinct
        ids and can OOM the driver on very large datasets.
        "hash" assigns each row's `source_group`/`target_group` using `hash(id) % num_groups`
        entirely within Spark, with no driver collect and no join. It scales to very large
        datasets but does not balance group sizes, so a small number of extremely
        high-degree ids (supernodes) can produce unbalanced groups. `null` ids are assigned a
        `null` group under both strategies.

    Returns
    -------
    DataFrame
        The Spark DataFrame with added columns `source_group`, `target_group` and `group`.
    """

    verify_spark_version(spark_session=spark_dataframe.sparkSession)

    if strategy == "hash":
        final_sdf = spark_dataframe.withColumn(
            "source_group", hash_group_column(source_col, num_groups)
        ).withColumn("target_group", hash_group_column(target_col, num_groups))

        return create_group_column_from_source_and_target_groups(final_sdf)

    # to create buckets
    # run over source and target INDEPENDENTLY
    # group by and count
    source_count_sdf = create_value_counts_dataframe(
        spark_dataframe=spark_dataframe, grouping_column=source_col
    )
    target_count_sdf = create_value_counts_dataframe(
        spark_dataframe=spark_dataframe, grouping_column=target_col
    )

    # iterate through the values in max -> min order ex: [{key: Amazon, value_count: 100000}, ...]
    # find most-empty bucket (num_groups) and place value in it and increment bucket value by value_count
    # # track with 2 separate hash maps
    source_groupings_sdf = create_value_groupings(
        value_counts_spark_dataframe=source_count_sdf,
        num_groups=num_groups,
        grouping_column=source_col,
    )
    target_groupings_sdf = create_value_groupings(
        value_counts_spark_dataframe=target_count_sdf,
        num_groups=num_groups,
        grouping_column=target_col,
    )

    final_sdf = spark_dataframe.join(
        other=source_groupings_sdf.withColumnRenamed("group", "source_group"),
        on=(spark_dataframe[source_col] == source_groupings_sdf.value),
        how="left",
    ).drop(source_groupings_sdf.value)
    final_sdf = final_sdf.join(
        other=target_groupings_sdf.withColumnRenamed("group", "target_group"),
        on=(spark_dataframe[target_col] == target_groupings_sdf.value),
        how="left",
    ).drop(target_groupings_sdf.value)

    final_sdf = final_sdf.drop("value")

    final_sdf = create_group_column_from_source_and_target_groups(final_sdf)

    return final_sdf
