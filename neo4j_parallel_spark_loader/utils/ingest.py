import warnings
from typing import Any, Dict, Literal, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_set, count, when


def ingest_spark_dataframe(
    spark_dataframe: DataFrame,
    save_mode: Literal["Overwrite", "Append"],
    options: Dict[str, Any],
    num_groups: Optional[int] = None,
    on_null_batch: Literal["raise", "skip"] = "raise",
) -> None:
    """
    Saves a Spark DataFrame in multiple batches based on the 'batch' column values.
    Each batch is partitioned by 'group' column and saved according to the provided options.
    Ingest will be performed in parallel according to the `group` column values.

    Parameters
    ----------
    spark_dataframe : DataFrame
        DataFrame containing 'batch' and 'grouping' columns.
        The 'batch' column is used to split the data into separate batches.
        The 'group' column is used for partitioning each batch.
    save_mode : Literal["append", "overwrite"]
        The Spark `mode` to use when ingesting data.
    options : Dict[str, Any]
        Dictionary of options to configure the DataFrame writer.
        Refer to example for more information.
    num_groups: Optional[int], optional
        The number of partitions to split Spark DataFrame into.
        If not provided, then will be calculated.
        It is more efficient to pass this parameter explicitly. By default None
    on_null_batch : Literal["raise", "skip"], optional
        What to do when some rows have a `null` value in the `batch` column. A `null` batch
        means the row could not be assigned to a group, which happens when a node id column
        (or the partition column for predefined components) is `null`. Such rows cannot be
        written as relationships and are never part of any batch.
        "raise" (the default) raises a `ValueError` reporting the number of affected rows
        before anything is written to Neo4j. "skip" emits a warning with the count and
        ingests the remaining rows. By default "raise"

    Example
    -------
    >>> options = {
            "relationship": "BOUGHT",
            "relationship.save.strategy": "keys",
            "relationship.source.save.mode": "Match",
            "relationship.source.labels": ":Customer",
            "relationship.source.node.keys": "customerID:id",
            "relationship.target.save.mode": "Match",
            "relationship.target.labels": ":Product",
            "relationship.target.node.keys": "customerID:id",
            "relationship.properties": "quantity,order"
        }
    >>> save_batches(my_spark_df, "Append", options)

    Note
    ----
    - The input DataFrame must contain 'batch' and 'grouping' columns
    - Each unique value in the 'batch' column will create a separate save operation
    - Uses the Neo4j Spark Connector for writing data to Neo4j
    - Rows with a `null` `batch` are handled according to `on_null_batch`
    """

    assert save_mode in {
        "Append",
        "Overwrite",
    }, "`save_mode` must be either 'Append' or 'Overwrite'"
    assert (
        "batch" in spark_dataframe.columns
    ), "Spark DataFrame must contain column `batch`"
    assert (
        "group" in spark_dataframe.columns
    ), "Spark DataFrame must contain column `group`"
    assert on_null_batch in {
        "raise",
        "skip",
    }, "`on_null_batch` must be either 'raise' or 'skip'"

    # Collect the distinct batch values and count null-batch rows in a single pass.
    # `collect_set` silently discards nulls, so without the explicit count any row whose
    # batch is null would never be written and nobody would be told.
    batch_list, null_batch_count = spark_dataframe.select(
        collect_set("batch"),
        count(when(col("batch").isNull(), 1)),
    ).first()

    if null_batch_count > 0:
        message = (
            f"{null_batch_count} row(s) have a null `batch` and cannot be ingested. "
            "This happens when a node id column (or the partition column for predefined "
            "components) is null, so the row was never assigned to a group. Filter or "
            "repair these rows before grouping, or pass `on_null_batch='skip'` to ingest "
            "the remaining rows and drop these."
        )
        if on_null_batch == "raise":
            raise ValueError(message)
        warnings.warn(message)

    batches = [
        spark_dataframe.filter(col("batch") == batch_value)
        for batch_value in batch_list
    ]

    num_groups = num_groups or spark_dataframe.select("group").distinct().count()

    # write batches serially to Neo4j database
    for batch in batches:
        (
            batch.repartition(num_groups, "group")  # define parallel groups for ingest
            .write.mode(save_mode)
            .format("org.neo4j.spark.DataSource")
            .options(**options)
            .save()
        )
