import logging
import sys
from typing import Any, Dict, Literal, Optional

from pyspark.sql import DataFrame

from ._scheduling import _release_staging

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
    unpersist: bool = True,
) -> None:
    """Write a scheduled Spark DataFrame to Neo4j without lock collisions.

    The input must contain the ``batch`` and ``group`` columns produced by
    ``v2.grouping.group_and_batch_spark_dataframe``.

    Grouping prepares and caches the batches before this function is called.
    Shipping does not collect relationship rows on the driver.

    Batches are written serially to provide a collision barrier. Within each
    batch, groups have disjoint endpoint buckets and are written in parallel.
    Grouping normally assigns each group its own populated Spark partition.
    If the fallback partition-key search is exhausted, batches are partitioned
    by group instead and may include empty partitions.

    The grouping metadata supplies Neo4j's ``batch.size`` unless it is already
    present in ``options``. Caches and loader-owned staging files are released
    by default, including when a write fails. Set ``unpersist=False`` to retain
    both for another write pass or retry.

    Parameters
    ----------
    batches : list[DataFrame]
        Ordered batches returned by ``group_and_batch_spark_dataframe`` or
        restored after a restart with ``load_staged_batches``.
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
        Skipped batches are also unpersisted when ``unpersist=True``. Retrying
        a partly completed batch can repeat writes, so use an idempotent query. Staged
        batches can only be retried if the previous call used unpersist=False;
        otherwise rebuild them because their staging files have been removed.
    unpersist : bool, default True
        Release all batch caches, including skipped and unwritten batches,
        on completion or failure. Delete loader-owned staging directories
        once all their original batches have been released. When False,
        retain both caches and staging files even on failure; the caller
        owns cleanup if the run is abandoned. Use False for a node write followed
        by a relationship write, and True on the final pass. If the first
        pass fails, release caches and remove staging when abandoning the run.
        Staged DataFrames must not be reused after cleanup.
    """

    if save_mode not in {"Append", "Overwrite"}:
        raise ValueError("save_mode must be either 'Append' or 'Overwrite'")
    if not isinstance(unpersist, bool):
        raise TypeError("unpersist must be a boolean")

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
                if unpersist:
                    batch.unpersist(blocking=False)
    finally:
        # Release batches not reached when a write fails.
        if unpersist:
            failed = sys.exc_info()[0] is not None
            try:
                for batch in batches[cleanup_from:]:
                    batch.unpersist(blocking=False)
            finally:
                try:
                    _release_staging(batches)
                except Exception:
                    if not failed:
                        raise
                    logging.exception("Could not clean staging after a failed write")
