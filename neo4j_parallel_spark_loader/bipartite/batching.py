from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def create_ingest_batches_from_groups(
    spark_dataframe: DataFrame, known_group_count: Optional[int] = None
) -> DataFrame:
    """
    Create batches for ingest into Neo4j.
    Add a `batch` column to the Spark DataFrame identifying which batch the group in that row belongs to.
    Remove `source_group` and `target_group` columns.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    known_group_count : Optional[int], optional
        The total number of possible groups, if already known (e.g. `num_groups` when using
        the "hash" grouping strategy). When provided, the `distinct().count()` passes over
        `source_group`/`target_group` are skipped. By default None.

    Returns
    -------
    DataFrame
        The Spark DataFrame with `batch` column added.
    """

    # assert that source_group and target_group exist in the dataframe
    # assert that the column types for above are IntegerType()

    if known_group_count is not None:
        num_colors = known_group_count
    else:
        source_group_count = spark_dataframe.select("source_group").distinct().count()

        target_group_count = spark_dataframe.select("target_group").distinct().count()

        num_colors = max(source_group_count, target_group_count)

    spark_dataframe = spark_dataframe.withColumn(
        "batch", (col("source_group") + col("target_group")) % num_colors
    ).drop(spark_dataframe.source_group, spark_dataframe.target_group)

    return spark_dataframe


def plan_ingest_batches(
    source_group_count: int, target_group_count: int
) -> Dict[int, List[str]]:
    """
    List the groups in each batch that `create_ingest_batches_from_groups` produces when
    `source_group` values are drawn from `[0, source_group_count)`, `target_group` values from
    `[0, target_group_count)`, and `known_group_count` is the larger of the two.
    """

    num_colors = max(source_group_count, target_group_count)
    batches: Dict[int, List[str]] = {}
    for source_group in range(source_group_count):
        for target_group in range(target_group_count):
            batch = (source_group + target_group) % num_colors
            batches.setdefault(batch, []).append(f"{source_group} --> {target_group}")

    return batches
