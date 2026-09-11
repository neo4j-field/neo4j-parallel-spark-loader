from typing import Literal

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, greatest, least, lit, when

from ..utils.grouping import (
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
    It is recommended to have `num_groups` <= (2 * the number of workers available in the Spark cluster) for monopartite grouping.

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
        "greedy" collects distinct id counts (combining source and target ids) to the driver
        and greedily bin-packs them into balanced groups. This scales poorly with the number
        of distinct ids and can OOM the driver on very large datasets.
        "hash" assigns each row's `source_group`/`target_group` using `hash(id) % num_groups`
        entirely within Spark, with no driver collect and no join. The same expression is
        applied to `source_col` and `target_col` so a node ID lands in the same group whether
        it appears as a source or a target. It scales to very large datasets but does not
        balance group sizes, so a small number of extremely high-degree ids (supernodes) can
        produce unbalanced groups. A row with a `null` id on either side is assigned a `null`
        `group` under both strategies.

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

        return _create_group_column(final_sdf)

    # stack source and target
    # group by and count
    counts_df = create_value_counts_dataframe(
        spark_dataframe=spark_dataframe, source_col=source_col, target_col=target_col
    )

    keys_sdf = create_value_groupings(
        value_counts_spark_dataframe=counts_df,
        num_groups=num_groups,
        grouping_column="combined_col",
    )

    final_sdf = (
        spark_dataframe.join(
            other=keys_sdf.withColumnRenamed("group", "source_group"),
            on=(spark_dataframe[source_col] == keys_sdf.value),
            how="left",
        )
        .drop(keys_sdf.value)
        .join(
            other=keys_sdf.withColumnRenamed("group", "target_group"),
            on=(spark_dataframe[target_col] == keys_sdf.value),
            how="left",
        )
        .drop(keys_sdf.value)
        .drop("value")
    )

    return _create_group_column(final_sdf)


def _create_group_column(spark_dataframe: DataFrame) -> DataFrame:
    """
    Add the undirected `group` column `"{min} -- {max}"` from `source_group` and `target_group`.

    `least` and `greatest` skip `null` arguments, so without an explicit check a row with a
    `null` node id on one side would land in a real self-loop group such as `"3 -- 3"` and be
    written to Neo4j. A relationship row with a `null` endpoint cannot be loaded, so the row is
    given a `null` group (and therefore a `null` batch) instead, matching the bipartite scenario
    and `hash_group_column`. `ingest_spark_dataframe` reports these rows via `on_null_batch`.
    """

    return spark_dataframe.withColumn(
        "group",
        when(
            col("source_group").isNull() | col("target_group").isNull(),
            lit(None),
        ).otherwise(
            concat(
                least("source_group", "target_group"),
                lit(" -- "),
                greatest("source_group", "target_group"),
            )
        ),
    )


def create_value_counts_dataframe(
    spark_dataframe: DataFrame, source_col: str, target_col: str
) -> DataFrame:
    """
    Create a `count` column counting the number of times a value appears in either the `source_col` or `target_col` column.

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

    Returns
    -------
    DataFrame
        A Spark DataFrame with columns `combined_col` and `count`.
    """

    combined_df = (
        spark_dataframe.select(source_col)
        .withColumnsRenamed({source_col: "combined_col"})
        .unionAll(
            spark_dataframe.select(target_col).withColumnsRenamed(
                {target_col: "combined_col"}
            )
        )
    )

    counts_df = combined_df.groupBy("combined_col").count()

    return counts_df
