import logging
import warnings
from typing import Any, Dict, List, Literal, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, create_map, hash, lit, pmod
from pyspark.sql.functions import min as spark_min

logger = logging.getLogger(__name__)

_GROUP_KEY_COLUMN = "__neo4j_parallel_group_key"


def ingest_spark_dataframe(
    spark_dataframe: DataFrame,
    save_mode: Literal["Overwrite", "Append"],
    options: Dict[str, Any],
    num_groups: Optional[int] = None,
    on_null_batch: Literal["raise", "skip"] = "raise",
    checkpoint_path: Optional[str] = None,
    sort_columns: Optional[List[str]] = None,
) -> None:
    """
    Saves a Spark DataFrame in multiple batches based on the 'batch' column values.
    Each batch is partitioned by 'group' column and saved according to the provided options.
    Ingest will be performed in parallel according to the `group` column values.

    Within a batch every group is written from its own Spark partition. Spark places rows in
    partitions by hashing the partitioning column, and hashing the `group` values directly
    leaves some partitions empty and stacks several groups on others. Instead, each group is
    mapped to a long key that is known to hash to a distinct partition, so a batch with `k`
    groups runs as exactly `k` parallel writers.

    Parameters
    ----------
    spark_dataframe : DataFrame
        DataFrame containing 'batch' and 'group' columns.
        The 'batch' column is used to split the data into separate batches.
        The 'group' column is used for partitioning each batch.
    save_mode : Literal["append", "overwrite"]
        The Spark `mode` to use when ingesting data.
    options : Dict[str, Any]
        Dictionary of options to configure the DataFrame writer.
        Refer to example for more information.
    num_groups: Optional[int], optional
        Deprecated and ignored. The number of partitions for each batch is now the number of
        groups observed in that batch. Accepted for backward compatibility. By default None
    on_null_batch : Literal["raise", "skip"], optional
        What to do when some rows have a `null` `batch` or `group`. This happens when a node id
        column (or the partition column for predefined components) is `null`, so the row could
        not be assigned to a group. Such rows cannot be written as relationships.
        "raise" (the default) raises a `ValueError` reporting the number of affected rows
        before anything is written to Neo4j. "skip" emits a warning with the count and
        ingests the remaining rows. By default "raise"
    checkpoint_path : Optional[str], optional
        When provided, the DataFrame is first written once to this location as Parquet
        partitioned by `batch`, and the batches are then read back from there. Without a
        checkpoint each batch is a filter over the input DataFrame, which recomputes the whole
        input plan once per batch. With a checkpoint each batch reads only its own files.
        The checkpoint also allows a failed load to be resumed: read the Parquet, filter to
        the remaining `batch` values, and pass the result to this function again. Any existing
        data at the path is overwritten. Rows with a `null` batch are written under the
        `batch=__HIVE_DEFAULT_PARTITION__` directory where they can be inspected. The caller
        is responsible for deleting the checkpoint after the load. By default None
    sort_columns : Optional[List[str]], optional
        Columns to sort each group's rows by before they are written, typically the source
        node id column. Hash grouping leaves rows in effectively random order, so consecutive
        relationships in a transaction touch unrelated node records and relationship chains
        and dirty a fresh page almost every row. Sorting by the source id makes consecutive
        rows update the same node record and chain head, so each transaction dirties far
        fewer pages, checkpoints have less to flush, and commits contend less with checkpoint
        IO. The sort is local to each partition and does not change which rows are in which
        group or batch, so the deadlock-free guarantee is unaffected. By default None (no sort)

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
    >>> ingest_spark_dataframe(my_spark_df, "Append", options, checkpoint_path="s3://bucket/tmp/load/")

    Note
    ----
    - The input DataFrame must contain 'batch' and 'group' columns
    - Each unique value in the 'batch' column will create a separate save operation
    - Uses the Neo4j Spark Connector for writing data to Neo4j
    - Rows with a `null` `batch` or `group` are handled according to `on_null_batch`
    - Progress is reported through the `neo4j_parallel_spark_loader.utils.ingest` logger at
      INFO level
    - Pass `sort_columns=[source_col]` to improve write locality on Neo4j for large loads
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
    for column in sort_columns or []:
        assert (
            column in spark_dataframe.columns
        ), f"sort column `{column}` is not in the Spark DataFrame"

    spark: SparkSession = spark_dataframe.sparkSession

    if checkpoint_path is not None:
        logger.info("Writing checkpoint partitioned by batch to %s", checkpoint_path)
        spark_dataframe.write.mode("overwrite").partitionBy("batch").parquet(
            checkpoint_path
        )
        spark_dataframe = spark.read.parquet(checkpoint_path)
        logger.info("Checkpoint written")

    # One pass collects everything the driver needs: the distinct (batch, group) pairs with
    # their row counts, and the number of rows that could not be assigned to a batch or group.
    # Only this small schedule is collected, never the relationship rows themselves.
    schedule_rows = (
        spark_dataframe.groupBy("batch", "group")
        .agg(count(lit(1)).alias("rows"))
        .collect()
    )

    null_row_count = 0
    schedule: Dict[Any, Dict[Any, int]] = {}
    for row in schedule_rows:
        if row["batch"] is None or row["group"] is None:
            null_row_count += row["rows"]
        else:
            schedule.setdefault(row["batch"], {})[row["group"]] = row["rows"]

    if null_row_count > 0:
        message = (
            f"{null_row_count} row(s) have a null `batch` or `group` and cannot be ingested. "
            "This happens when a node id column (or the partition column for predefined "
            "components) is null, so the row was never assigned to a group. Filter or "
            "repair these rows before grouping, or pass `on_null_batch='skip'` to ingest "
            "the remaining rows and drop these."
        )
        if on_null_batch == "raise":
            raise ValueError(message)
        warnings.warn(message)

    group_type = spark_dataframe.schema["group"].dataType
    keys_by_count: Dict[int, List[int]] = {}
    total_batches = len(schedule)

    # write batches serially to Neo4j database
    for position, batch_value in enumerate(sorted(schedule), start=1):
        groups = sorted(schedule[batch_value])
        partition_count = len(groups)
        batch_rows = sum(schedule[batch_value].values())

        if partition_count not in keys_by_count:
            keys_by_count[partition_count] = _partition_keys(spark, partition_count)
        keys = keys_by_count[partition_count]

        # map every group in this batch to a key that lands in its own partition
        group_to_key = create_map(
            *[
                expression
                for group, key in zip(groups, keys)
                for expression in (lit(group).cast(group_type), lit(key).cast("long"))
            ]
        )

        logger.info(
            "Ingesting batch %s (%d/%d): %d groups in parallel, %d rows",
            batch_value,
            position,
            total_batches,
            partition_count,
            batch_rows,
        )

        batch_df = (
            spark_dataframe.filter(col("batch") == batch_value)
            .filter(col("group").isNotNull())
            .withColumn(_GROUP_KEY_COLUMN, group_to_key[col("group")])
            .repartition(partition_count, col(_GROUP_KEY_COLUMN))
        )
        if sort_columns:
            # sorted within each partition only; the partitioning (one group per
            # partition) is preserved, so groups still never share a writer
            batch_df = batch_df.sortWithinPartitions(*sort_columns)

        (
            batch_df.drop(_GROUP_KEY_COLUMN)
            .write.mode(save_mode)
            .format("org.neo4j.spark.DataSource")
            .options(**options)
            .save()
        )

        logger.info("Finished batch %s (%d/%d)", batch_value, position, total_batches)


def _partition_keys(spark: SparkSession, partition_count: int) -> List[int]:
    """
    Find `partition_count` long values that `DataFrame.repartition(partition_count, column)`
    places in `partition_count` distinct partitions, ordered by partition id.

    Spark's hash partitioning assigns a row to partition `pmod(hash(column), partition_count)`
    using the same Murmur3 hash exposed as the `hash` SQL function, so the assignment can be
    computed ahead of time. Candidate values are drawn from `spark.range` and the search widens
    until every partition has a representative. This runs a tiny Spark job, independent of
    the data being ingested.
    """

    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    if partition_count == 1:
        return [0]

    candidate_count = max(32, partition_count * 16)
    for _ in range(8):
        found = (
            spark.range(candidate_count)
            .select(
                col("id"),
                pmod(hash(col("id")), lit(partition_count)).alias("partition"),
            )
            .groupBy("partition")
            .agg(spark_min("id").alias("key"))
            .collect()
        )
        if len(found) == partition_count:
            return [row["key"] for row in sorted(found, key=lambda r: r["partition"])]
        candidate_count *= 2

    raise RuntimeError(
        f"Could not find distinct partition keys for {partition_count} partitions "
        f"after searching {candidate_count // 2} candidates"
    )
