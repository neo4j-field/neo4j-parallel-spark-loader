from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

from ..utils.grouping import create_value_counts_dataframe, create_value_groupings
from ..utils.verify_spark import verify_spark_version


def create_node_groupings(
    spark_dataframe: DataFrame, partition_col: str, num_groups: int
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

    Returns
    -------
    DataFrame
        The Spark DataFrame with added column `group`.
    """

    verify_spark_version(spark_session=spark_dataframe.sparkSession)

    # to create buckets
    # run over partition_col
    # group by and count
    value_counts_sdf = create_value_counts_dataframe(
        spark_dataframe=spark_dataframe, grouping_column=partition_col
    )
    # iterate through the values in max -> min order ex: [{key: Amazon, value_count: 100000}, ...]
    # find most-empty bucket (num_groups) and place value in it and increment bucket value by value_count
    # # track with 2 separate hash maps
    value_groupings_sdf = create_value_groupings(
        value_counts_spark_dataframe=value_counts_sdf,
        num_groups=num_groups,
        grouping_column=partition_col,
    )

    final_sdf = spark_dataframe.join(
        other=broadcast(value_groupings_sdf),
        on=(spark_dataframe[partition_col] == value_groupings_sdf.value),
        how="left",
    ).drop(value_groupings_sdf.value)

    final_sdf = final_sdf.drop("value")

    return final_sdf
