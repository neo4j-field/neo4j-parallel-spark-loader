"""Reopen a staged v2 run without rebuilding its schedule."""

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructType

from ._scheduling import _StagingDirectory, _build_batches, _read_staged


def load_staged_batches(
    spark: SparkSession,
    staging_path: str,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
) -> list[DataFrame]:
    """Restore batches from an existing v2 staging directory after a restart.

    Reads the saved schema, batch order, counts, and partitioning strategy.
    Does not access the original input, regenerate group keys, rewrite staging,
    or scan relationship rows to reconstruct the schedule. Returned batches
    are lazy, so shipping with ``resume_from`` does not read skipped batches.
    A selected batch reads its own Parquet files and reapplies final partitioning;
    with caching enabled, that result is retained for subsequent write passes.

    Requires staging written with resume metadata by the current v2 loader.
    Direct partition IDs written on Spark 4.1+ require Spark 4.1+ when loading.
    The original Neo4j transaction-size metadata is restored.

    Shipping with ``unpersist=False`` keeps caches and staging for another
    attempt. ``unpersist=True`` deletes staging after all batches are released,
    including on write failure. Invalid or unreadable staging is never deleted
    by this function. Do not load the same staging path into concurrent runs:
    each run's shipping cleanup assumes exclusive use of the directory.

    Parameters
    ----------
    spark: ```SparkSession```
        The spark session to use
    staging_path : str
        The path to the staging directory, or location.
        `s3://` also works on runtimes configured to support that scheme.
    cache: { See ```pyspark.sql.DataFrame.persist``` and ```pyspark.StorageLevel```}
        Controls persistence of the returned batch DataFrames and, without
        staging_path, the temporary intermediate dataset.
        Defaults to ```StorageLevel.MEMORY_AND_DISK```, which uses disk and memory, with no replication.
        Use ```StorageLevel.NONE``` to disable both caches, if needed.
    """
    staging = _StagingDirectory(spark, staging_path)
    manifest = staging.read_manifest()
    try:
        if (
            manifest["format"] != "neo4j_parallel_spark_loader.v2"
            or manifest["version"] != 1
            or manifest["partitioning"] not in {"direct", "hash"}
        ):
            raise ValueError("Unsupported staging format")
        direct = manifest["partitioning"] == "direct"
        schema = StructType.fromJson(manifest["schema"])
        if (
            not isinstance(schema["batch"].dataType, StringType)
            or not isinstance(schema["group"].dataType, StringType)
            or not isinstance(
                schema["groupKey"].dataType, IntegerType if direct else LongType
            )
        ):
            raise ValueError("Invalid staging schema")
        schedule = manifest["schedule"]
        if not isinstance(schedule, list):
            raise ValueError("Invalid staging schedule")
        seen = set()
        for entry in schedule:
            if (
                not isinstance(entry["batch"], str)
                or entry["batch"] in seen
                or not isinstance(entry["partition_count"], int)
                or isinstance(entry["partition_count"], bool)
                or entry["partition_count"] <= 0
                or entry["use_group"] not in (0, 1)
                or (direct and entry["use_group"])
            ):
                raise ValueError("Invalid staging schedule")
            seen.add(entry["batch"])
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise ValueError(f"Invalid v2 resume metadata at {staging_path}") from error

    scheduled = _read_staged(spark, staging_path, schema)
    if direct and not callable(getattr(type(scheduled), "repartitionById", None)):
        raise ValueError(
            "This staging directory uses direct partition IDs; Spark 4.1+ is required"
        )
    return _build_batches(
        scheduled, schedule, cache, direct=direct, staging=staging, materialize=False
    )
