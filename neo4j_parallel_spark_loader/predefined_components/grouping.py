from typing import Literal

from pyspark.sql import DataFrame

from ..utils.grouping import (
    GroupingResult,
    apply_key_groupings,
    create_key_groupings,
    value_key_column,
)
from ..utils.hash_grouping import hash_group_column
from ..utils.verify_spark import verify_spark_version


def create_node_groupings(
    spark_dataframe: DataFrame,
    partition_col: str,
    num_groups: int,
    strategy: Literal["greedy", "hash"] = "greedy",
) -> DataFrame:
    """
    Create node groupings for parallel ingest into Neo4j.
    Add a `group` column to the Spark DataFrame identifying which group the row belongs in.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    partition_col : str
        The desired column to partition on.
    num_groups : int
        The desired number of groups to generate. The process may generate less groups as necessary.
    strategy : Literal["greedy", "hash"], optional
        The grouping strategy to use. By default "greedy".
        "greedy" counts rows per distinct `partition_col` value, collects the counts to the
        driver keyed by a 64-bit hash of the value, and greedily bin-packs them into balanced
        groups. This scales poorly with the number of distinct values and can OOM the driver
        on very large datasets.
        "hash" assigns each row's `group` using `hash(partition_col) % num_groups` entirely
        within Spark, with no driver collect and no join. It scales to very large datasets
        but does not balance group sizes, so a small number of extremely large components
        (supernodes) can produce unbalanced groups. `null` values in `partition_col` are
        assigned a `null` group under both strategies.

    Returns
    -------
    DataFrame
        The Spark DataFrame with added column `group`.
    """

    return _create_node_groupings(
        spark_dataframe=spark_dataframe,
        partition_col=partition_col,
        num_groups=num_groups,
        strategy=strategy,
    ).dataframe


def _create_node_groupings(
    spark_dataframe: DataFrame,
    partition_col: str,
    num_groups: int,
    strategy: Literal["greedy", "hash"] = "greedy",
) -> GroupingResult:
    """`create_node_groupings`, also returning what grouping learned for the ingest plan."""

    verify_spark_version(spark_session=spark_dataframe.sparkSession)

    if strategy == "hash":
        return GroupingResult(
            dataframe=spark_dataframe.withColumn(
                "group", hash_group_column(partition_col, num_groups)
            ),
            group_counts=[num_groups],
        )

    # count rows per partition value key, bin-pack the keys into balanced groups on the
    # driver, then join the groups back on the key
    key = value_key_column(partition_col)
    key_groupings = create_key_groupings(
        spark_dataframe=spark_dataframe, key_sets=[[key]], num_groups=num_groups
    )
    [(mapping_sdf, group_count)] = key_groupings.mappings

    return GroupingResult(
        dataframe=apply_key_groupings(
            spark_dataframe=spark_dataframe,
            key_column=key,
            mapping_sdf=mapping_sdf,
            group_count=group_count,
            output_column="group",
        ),
        group_counts=[group_count],
        total_rows=key_groupings.total_rows,
        null_rows=key_groupings.null_rows,
    )
