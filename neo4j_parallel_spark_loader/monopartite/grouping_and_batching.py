from pyspark.sql import DataFrame

from .batching import create_ingest_batches_from_groups
from .grouping import create_node_groupings


def group_and_batch_spark_dataframe(
    spark_dataframe: DataFrame, source_col: str, target_col: str, num_groups: int
) -> DataFrame:
    """
    Create node groupings and batches for parallel ingest into Neo4j.
    Add `source_group`, `target_group`, `final_group` and `batch` columns to the Spark DataFrame identifying which grous and batch the row belongs in.
    `final_group` is a concatenation of the source and target group values.
    `final_group` and `batch` are utilized during ingestion.

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
        The Spark DataFrame with added columns `source_group`, `target_group`, `final_group` and `batch`.
    """

    grouped_sdf = create_node_groupings(
        spark_dataframe=spark_dataframe,
        source_col=source_col,
        target_col=target_col,
        num_groups=num_groups,
    )
    return create_ingest_batches_from_groups(spark_dataframe=grouped_sdf)
