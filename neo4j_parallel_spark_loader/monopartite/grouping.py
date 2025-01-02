from pyspark.sql import DataFrame

from ..utils.grouping import (
    create_final_group_column_from_source_and_target_groups,
    create_value_groupings,
)


def create_node_groupings(
    spark_dataframe: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
) -> DataFrame:
    """
    Create node groupings for parallel ingest into Neo4j.
    Add `source_group`, `target_group` and `final_group` columns to the Spark DataFrame identifying which groups the row belongs in.
    `final_group` is a concatenation of the source and target group values.
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

    Returns
    -------
    DataFrame
        The Spark DataFrame with added columns `source_group`, `target_group` and `final_group`.
    """

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

    final_sdf = spark_dataframe.join(
        other=keys_sdf.withColumnRenamed("group", "source_group"),
        on=(spark_dataframe[source_col] == keys_sdf.value),
        how="left",
    ).drop(keys_sdf.value)
    final_sdf = final_sdf.join(
        other=keys_sdf.withColumnRenamed("group", "target_group"),
        on=(spark_dataframe[target_col] == keys_sdf.value),
        how="left",
    ).drop(keys_sdf.value)

    final_sdf = final_sdf.drop("value")

    final_sdf = create_final_group_column_from_source_and_target_groups(final_sdf)

    return final_sdf


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
