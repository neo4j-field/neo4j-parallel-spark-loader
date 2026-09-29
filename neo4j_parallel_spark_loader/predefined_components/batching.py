from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def create_ingest_batches_from_groups(spark_dataframe: DataFrame) -> DataFrame:
    """
    Create batches for ingest into Neo4j.
    Add a `batch` column to the Spark DataFrame identifying which batch the group in that row belongs to.
    In the case of `predefined components` all groups will be in the same batch.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.

    Returns
    -------
    DataFrame
        The Spark DataFrame with a `batch` column.
    """

    return spark_dataframe.withColumn("batch", lit(0))


def plan_ingest_batches(group_count: int) -> Dict[int, List[int]]:
    """
    List the groups in each batch that `create_ingest_batches_from_groups` produces when group
    values are drawn from `[0, group_count)`: all groups are in batch 0.
    """

    return {0: list(range(group_count))} if group_count > 0 else {}
