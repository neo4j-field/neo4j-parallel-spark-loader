from pyspark.sql import DataFrame

from .batching import create_ingest_batches_from_groups
from .grouping import create_node_groupings


def group_and_batch_spark_dataframe(
    spark_dataframe: DataFrame, partition_col: str, num_groups: int
) -> DataFrame:
    """
    Create node groupings and batches for parallel ingest into Neo4j.
    Add `group` and `batch` columns to the Spark DataFrame identifying which grous and batch the row belongs in.
    `group` is a concatenation of the source and target group values.
    `group` and `batch` are utilized during ingestion.

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
        The Spark DataFrame with added columns `group` and `batch`.
    """

    grouped_sdf = create_node_groupings(
        spark_dataframe=spark_dataframe,
        partition_col=partition_col,
        num_groups=num_groups,
    )
    return create_ingest_batches_from_groups(spark_dataframe=grouped_sdf)
