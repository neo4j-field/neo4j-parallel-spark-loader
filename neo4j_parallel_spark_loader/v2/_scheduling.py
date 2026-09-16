"""Shared, JVM-only schedule construction and batch materialization."""

import json
import logging

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F


# Only generated group identifiers and integer keys are broadcast, never input
# properties. Larger schedules stay distributed and use Spark's join planner.
_BROADCAST_GROUP_LIMIT = 10_000


class _StagingDirectory:
    """Own a successfully written staging directory until all batches release it."""

    def __init__(self, session, path: str):
        self.session = session
        self.path = session._jvm.org.apache.hadoop.fs.Path(path)
        self.fs = self.path.getFileSystem(
            session.sparkContext._jsc.hadoopConfiguration()
        )
        self.pending = set()
        self.deleted = False

    def write_manifest(self, schema, schedule, direct):
        """Persist the schema and batch order after the Parquet write completes."""
        manifest = {
            "format": "neo4j_parallel_spark_loader.v2",
            "version": 1,
            "schema": schema.jsonValue(),
            "partitioning": "direct" if direct else "hash",
            "schedule": [row.asDict() for row in schedule],
        }
        path = self.session._jvm.org.apache.hadoop.fs.Path(
            self.path, "_neo4j_loader.json"
        )
        stream = self.fs.create(path, False)
        try:
            stream.write(bytearray(json.dumps(manifest).encode("utf-8")))
        finally:
            stream.close()

    def read_manifest(self):
        path = self.session._jvm.org.apache.hadoop.fs.Path(
            self.path, "_neo4j_loader.json"
        )
        if not self.fs.exists(path):
            raise ValueError(
                f"No v2 resume metadata found at {self.path}; "
                "create staging with the current v2 loader first"
            )
        stream = self.fs.open(path)
        try:
            return json.loads(
                self.session._jvm.org.apache.commons.io.IOUtils.toString(
                    stream, "UTF-8"
                )
            )
        finally:
            stream.close()

    def cleanup(self):
        if self.deleted:
            return
        if self.fs.exists(self.path) and not self.fs.delete(self.path, True):
            raise OSError(f"Could not delete Spark staging directory: {self.path}")
        self.deleted = True


def _release_staging(batches: list[DataFrame]):
    owners = set()
    for batch in batches:
        # Read Python-owned state only, never a DataFrame column or Spark
        # schema metadata that could supply an unrelated filesystem path.
        staging = vars(batch).get("_neo4j_staging")
        if staging is not None:
            owner, index = staging
            owner.pending.discard(index)
            owners.add(owner)
    first_error = None
    for owner in owners:
        if not owner.pending:
            try:
                owner.cleanup()
            except Exception as error:
                if first_error is None:
                    first_error = error
    if first_error is not None:
        raise first_error


def _rank_groups(grouped: DataFrame) -> tuple[DataFrame, list[Row]]:
    groups = grouped.select("batch", "group").distinct()
    # O(number of batches) driver metadata, rather than O(number of groups).
    counts = groups.groupBy("batch").count().collect()
    ranked = groups.select(
        "group",
        (F.row_number().over(Window.partitionBy("batch").orderBy("group")) - 1)
        .cast("int")
        .alias("ordinal"),
        F.count("group").over(Window.partitionBy("batch")).alias("partitionCount"),
    )
    return ranked, counts


def _join_group_keys(
    grouped: DataFrame, keys: DataFrame, group_count: int
) -> DataFrame:
    if group_count <= _BROADCAST_GROUP_LIMIT:
        keys = F.broadcast(keys)
    return grouped.drop("groupKey").join(keys, on="group", how="left")


def _read_staged(session, path, schema):
    return (
        session.read.schema(schema)
        .parquet(path)
        # File partition discovery drops metadata on partition columns.
        .withMetadata("batch", schema["batch"].metadata)
    )


def _build_batches(scheduled, schedule, cache, *, direct, staging, materialize):
    """Create batch plans; reloaded staging stays lazy so resume skips old reads."""
    batches = []
    try:
        for index, entry in enumerate(schedule, start=1):
            partition_count = entry["partition_count"]
            batch = scheduled.filter(F.col("batch") == entry["batch"])
            if direct:
                batch = batch.repartitionById(partition_count, F.col("groupKey"))
            else:
                batch = batch.repartition(
                    partition_count,
                    F.col("group" if entry["use_group"] else "groupKey"),
                )
            if cache != StorageLevel.NONE:
                batch = batch.persist(cache)
            if staging is not None:
                staging.pending.add(index)
                batch._neo4j_staging = (staging, index)
            batches.append(batch)
            if materialize and cache != StorageLevel.NONE:
                batch.count()
            logging.info(
                "Prepared batch %s/%s with %s groups",
                index,
                len(schedule),
                partition_count,
            )
        return batches
    except Exception:
        for batch in batches:
            batch.unpersist(blocking=False)
        raise


def _repartition_batches(
    grouped: DataFrame,
    cache: StorageLevel,
    *,
    direct: bool,
    staging_path: str | None = None,
) -> list[DataFrame]:
    # Spread different groups across tasks instead of funneling a whole batch
    # through one task. Sorting clusters each batch within cached columnar
    # blocks, enabling Spark's min/max cache filtering for per-batch reads.
    # Keep the temporary full copy on disk so it does not compete with the
    # final batch caches for executor storage memory.
    scheduled = None
    staging = None
    try:
        if staging_path is None:
            scheduled = (
                grouped.repartition("batch", "group")
                .sortWithinPartitions("batch")
                .persist(StorageLevel.DISK_ONLY)
            )
        else:
            # Resolve cleanup through Hadoop so local and remote paths work.
            # Claim ownership only after the write succeeds; an existing path
            # rejected by errorifexists must never be deleted.
            owner = _StagingDirectory(grouped.sparkSession, staging_path)
            grouped.repartition("batch", "group").write.mode(
                "errorifexists"
            ).partitionBy("batch").parquet(staging_path)
            staging = owner
            scheduled = _read_staged(grouped.sparkSession, staging_path, grouped.schema)
        logging.info("Collecting per-batch counts from the intermediate dataset.")
        schedule = (
            scheduled.groupBy("batch")
            .agg(
                F.countDistinct("group").alias("partition_count"),
                F.max(F.col("groupKey").isNull().cast("int")).alias("use_group"),
            )
            .orderBy("batch")
            .collect()
        )
        if staging is not None:
            staging.write_manifest(grouped.schema, schedule, direct)
        batches = _build_batches(
            scheduled,
            schedule,
            cache,
            direct=direct,
            staging=staging,
            materialize=True,
        )
        if staging is not None and not batches:
            staging.cleanup()
        return batches
    except Exception:
        if staging is not None:
            staging.cleanup()
        raise
    finally:
        if staging_path is None and scheduled is not None:
            scheduled.unpersist(blocking=True)
