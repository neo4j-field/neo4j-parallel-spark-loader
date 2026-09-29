from typing import Literal

from pyspark.sql import DataFrame

from ..utils.grouping import (
    GroupingResult,
    apply_key_groupings,
    create_group_column_from_source_and_target_groups,
    create_key_groupings,
    create_value_counts_dataframe,  # noqa: F401 re-exported for backward compatibility
    value_key_column,
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
        "greedy" counts rows per distinct source id and per distinct target id, collects the
        counts to the driver keyed by a 64-bit hash of the id, and greedily bin-packs them
        into balanced groups. This scales poorly with the number of distinct
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

    return _create_node_groupings(
        spark_dataframe=spark_dataframe,
        source_col=source_col,
        target_col=target_col,
        num_groups=num_groups,
        strategy=strategy,
    ).dataframe


def _create_node_groupings(
    spark_dataframe: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    strategy: Literal["greedy", "hash"] = "greedy",
) -> GroupingResult:
    """
    `create_node_groupings`, also returning what grouping learned for batching and the ingest
    plan. `group_counts` is `[source group count, target group count]`.
    """

    verify_spark_version(spark_session=spark_dataframe.sparkSession)

    if strategy == "hash":
        final_sdf = spark_dataframe.withColumn(
            "source_group", hash_group_column(source_col, num_groups)
        ).withColumn("target_group", hash_group_column(target_col, num_groups))

        return GroupingResult(
            dataframe=create_group_column_from_source_and_target_groups(final_sdf),
            group_counts=[num_groups, num_groups],
        )

    # bin-pack source and target keys INDEPENDENTLY, since a bipartite source id and target
    # id never refer to the same node
    source_key = value_key_column(source_col)
    target_key = value_key_column(target_col)
    key_groupings = create_key_groupings(
        spark_dataframe=spark_dataframe,
        key_sets=[[source_key], [target_key]],
        num_groups=num_groups,
    )
    [
        (source_mapping_sdf, source_group_count),
        (target_mapping_sdf, target_group_count),
    ] = key_groupings.mappings

    final_sdf = apply_key_groupings(
        spark_dataframe=spark_dataframe,
        key_column=source_key,
        mapping_sdf=source_mapping_sdf,
        group_count=source_group_count,
        output_column="source_group",
    )
    final_sdf = apply_key_groupings(
        spark_dataframe=final_sdf,
        key_column=target_key,
        mapping_sdf=target_mapping_sdf,
        group_count=target_group_count,
        output_column="target_group",
    )

    return GroupingResult(
        dataframe=create_group_column_from_source_and_target_groups(final_sdf),
        group_counts=[source_group_count, target_group_count],
        total_rows=key_groupings.total_rows,
        null_rows=key_groupings.null_rows,
    )
