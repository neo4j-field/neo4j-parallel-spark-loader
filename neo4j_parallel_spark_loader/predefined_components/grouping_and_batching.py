from typing import Literal, Tuple, Union

from pyspark.sql import DataFrame

from ..utils.ingest_plan import IngestPlan
from .batching import create_ingest_batches_from_groups, plan_ingest_batches
from .grouping import _create_node_groupings


def group_and_batch_spark_dataframe(
    spark_dataframe: DataFrame,
    partition_col: str,
    num_groups: int,
    strategy: Literal["greedy", "hash"] = "greedy",
    return_plan: bool = False,
) -> Union[DataFrame, Tuple[DataFrame, IngestPlan]]:
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
    strategy : Literal["greedy", "hash"], optional
        The grouping strategy to use. See `create_node_groupings` for details. By default "greedy".

    return_plan : bool, optional
        When True, also return an `IngestPlan` to pass to `ingest_spark_dataframe` as
        `plan`, which lets ingest skip its own pass over the DataFrame. With the "hash"
        strategy this runs one counting pass over the DataFrame here. By default False.

    Returns
    -------
    Union[DataFrame, Tuple[DataFrame, IngestPlan]]
        The Spark DataFrame with added columns `group` and `batch`, and the `IngestPlan` when
        `return_plan` is True.
    """

    grouping = _create_node_groupings(
        spark_dataframe=spark_dataframe,
        partition_col=partition_col,
        num_groups=num_groups,
        strategy=strategy,
    )
    batched_sdf = create_ingest_batches_from_groups(spark_dataframe=grouping.dataframe)
    if not return_plan:
        return batched_sdf

    grouping = grouping.with_row_counts([partition_col])
    [group_count] = grouping.group_counts
    plan = IngestPlan(
        batches=plan_ingest_batches(group_count),
        total_rows=grouping.total_rows,
        null_rows=grouping.null_rows,
    )
    return batched_sdf, plan
