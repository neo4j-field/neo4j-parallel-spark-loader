import logging
from typing import Any, Dict, Literal, Optional

from pyspark.sql import DataFrame

logging.basicConfig(
    force=True,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ingest_spark_dataframe(
    batches: list[DataFrame],
    save_mode: Literal["Overwrite", "Append"],
    options: Dict[str, Any] = {},
    resume_from: Optional[int] = 0,
) -> None:
    """Write a scheduled Spark DataFrame to Neo4j without lock collisions.

    The input must contain the ``batch`` and ``group`` columns produced by
    ``v2.grouping.group_and_batch_spark_dataframe``.

    Rows are shuffled and cached by batch once, preventing Spark from recomputing
    the complete input for every write round.

    Only the small set of distinct batch/group identifiers is collected
    on the driver; relationship rows remain distributed across the cluster.

    Batches are written serially to provide a collision barrier. Within each
    batch, groups have disjoint endpoint buckets and are written in parallel.
    Grouping normally assigns each group its own populated Spark partition.
    If the fallback partition-key search is exhausted, batches are partitioned
    by group instead and may include empty partitions.

    The grouping metadata supplies Neo4j's ``batch.size`` unless it is already
    present in ``options``. Cached data uses memory with disk spill and is always
    released, including when a write fails.

    Parameters
    ----------
    batches : list[DataFrame]
        Ordered batches returned by ``v2.grouping.group_and_batch_spark_dataframe``.
    save_mode : {"Overwrite", "Append"}
        Spark save mode passed to the Neo4j connector.
    options : dict
        Options passed to ``org.neo4j.spark.DataSource``.
    resume_from : int or None, default 0
        One-based batch number shown in the shipping logs, not the batch column
        value. ``None``, zero, and one start from the beginning. For a failure
        logged as batch 3/N, use ``resume_from=3`` to retry that batch. The same
        ordered input batches must be supplied. ``len(batches)`` retries the
        final batch; negative numbers or numbers above that limit are rejected.
        Skipped batches are also unpersisted. Retrying a partly completed batch
        can repeat writes, so use an idempotent query when resuming.
    """

    if save_mode not in {"Append", "Overwrite"}:
        raise ValueError("save_mode must be either 'Append' or 'Overwrite'")

    batch_number = 0 if resume_from is None else resume_from
    if isinstance(batch_number, bool) or not isinstance(batch_number, int):
        raise TypeError("resume_from must be an integer or None")
    if not 0 <= batch_number <= len(batches):
        raise ValueError(f"resume_from must be between 0 and {len(batches)}")
    start = max(0, batch_number - 1)

    if not batches:
        raise ValueError("No batches where passed to ```ingest_spark_dataframe```.")

    for batch in batches:
        if "batch" not in batch.columns:
            raise ValueError("Spark DataFrame must contain column `batch`")
        if "group" not in batch.columns:
            raise ValueError("Spark DataFrame must contain column `group`")

    total_batches = len(batches)
    cleanup_from = 0
    try:
        write_options = dict(options)
        batch_size = batches[start].schema["batch"].metadata.get("neo4j_batch_size")
        if batch_size is not None:
            write_options.setdefault("batch.size", str(batch_size))
        logging.info(f"Starting run with configuration: {write_options!s}")

        for index, batch in enumerate(batches):
            try:
                if index < start:
                    continue
                completed = index + 1
                logging.info(f"Started shipping batch {completed}/{total_batches}")
                (
                    batch.write.mode(save_mode)
                    .format("org.neo4j.spark.DataSource")
                    .options(**write_options)
                    .save()
                )
                logging.info(f"Finished shipping batch {completed}/{total_batches}")
            finally:
                cleanup_from = index + 1
                batch.unpersist(blocking=False)
    finally:
        # Release batches not reached when a write fails.
        for batch in batches[cleanup_from:]:
            batch.unpersist(blocking=False)
