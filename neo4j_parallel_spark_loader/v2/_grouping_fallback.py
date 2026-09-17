"""
```repartitionById``` was added in v4.1.0

Use calculated hashes as a fallback

"""

import json
import logging
import math

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    concat,
    greatest,
    least,
    lit,
    pmod,
    xxhash64,
)
from ._scheduling import _join_group_keys, _rank_groups, _repartition_batches

logging.basicConfig(
    force=True,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _partition_keys(spark: SparkSession, partition_count: int) -> list[int] | None:
    """Find one long key per partition in at most eight candidate scans.

    Start with n * (log(n) + 16) candidates, where n is the distinct group
    count in one batch, with a minimum of 32. Under uniform hashing, the
    logarithmic term accounts for covering every partition, and the margin
    of 16 provides headroom. Coverage is verified rather than assumed.
    Double the candidate budget on each retry, keeping a fixed attempt cap.
    Return None if the search is exhausted, so callers can partition by group.
    """
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    initial_candidate_count = max(
        32, math.ceil(partition_count * (math.log(partition_count) + 16))
    )
    max_attempts = 8
    for attempt in range(max_attempts):
        candidate_count = initial_candidate_count * (2**attempt)
        keys = (
            spark.range(candidate_count)
            .select("id", pmod(F.hash("id"), lit(partition_count)).alias("partition"))
            .groupBy("partition")
            .agg(F.min("id").alias("key"))
            .collect()
        )
        if len(keys) == partition_count:
            return [
                row["key"] for row in sorted(keys, key=lambda row: row["partition"])
            ]
    return None


# Bound driver-side hash-key metadata even when num_groups is very large.
# Batches beyond this budget use the existing group-hash fallback.
_MAX_PARTITION_KEYS = 100_000


def _guarantee_group_distinct_partitions(
    grouped: DataFrame, schedule: DataFrame, counts: list[Row], session: SparkSession
) -> DataFrame:
    """Join distributed group ordinals to a bounded set of collision-free keys."""
    key_rows = []
    remaining = _MAX_PARTITION_KEYS
    # A key vector is shared by all batches with the same group count.
    for count in sorted({entry["count"] for entry in counts}):
        if count > remaining:
            continue
        remaining -= count
        keys = _partition_keys(session, count)
        if keys is None:
            continue
        key_rows.extend(
            {"partitionCount": count, "ordinal": ordinal, "groupKey": key}
            for ordinal, key in enumerate(keys)
        )
    # JVM expressions avoid Python RDD serialization, including empty lookups.
    partition_keys = (
        session.range(1)
        .select(
            F.explode(
                F.from_json(
                    lit(json.dumps(key_rows)),
                    "array<struct<partitionCount:bigint,ordinal:int,groupKey:bigint>>",
                )
            ).alias("entry")
        )
        .select("entry.*")
    )
    group_keys = schedule.join(
        F.broadcast(partition_keys), on=["partitionCount", "ordinal"], how="left"
    ).select("group", "groupKey")
    # Uncovered counts retain null keys, selecting group-based repartitioning.
    return _join_group_keys(grouped, group_keys, sum(row["count"] for row in counts))


def _create_node_groupings_v2(
    df: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    batch_size: int,
) -> DataFrame:
    """Assign batches with disjoint endpoint buckets and long partition keys.

    Hash endpoints into ``num_groups`` buckets, then assign each unordered
    bucket pair to a batch by its sum modulo an odd colour count. Groups in
    one batch have disjoint endpoint buckets, including reverse edges and
    self-loops. Different batches must be written serially.

    Distinct groups are ranked within each batch in Spark. Collision-free
    hash keys are reused across batches of the same size. Driver metadata
    contains only batch counts and at most 100,000 partition keys. If key
    search or its metadata budget is exhausted, affected batches receive
    null keys and are repartitioned by group instead. That fallback preserves
    safety but may put several groups into one partition.

    Original properties are retained. Existing ``group``, ``batch`` and
    ``groupKey`` columns are replaced; ``batch`` carries the Neo4j transaction
    size as metadata. Small schedules are broadcast; larger schedules remain
    distributed. Relationship rows are never collected on the driver.
    """

    # Calculate the hash of each unique value in the column, and create a new column with those values
    # Do this for target and source columns
    # pmod(hash64(cell_value), divisor=num_groups)
    # i.e. for Row 1 sourceId:
    #  sourceIdHash = hash64(1) % 100 = 1
    #  if sourceIdHash < 0:
    #     sourceIdHash = sourceIdHash * -1
    #  targetIdHash = hash64(1) % 100 =
    #  if targetIdHash < 0:
    #     targetIdHash = targetIdHash * -1
    source_group = pmod(xxhash64(col(source_col)), lit(num_groups))
    target_group = pmod(xxhash64(col(target_col)), lit(num_groups))

    # Create and assign a column with the lowest target or source group
    low_group = least(source_group, target_group)  # 18
    # Create and assign a column with the highest target or source group
    high_group = greatest(source_group, target_group)  # 19

    # When num_groups is even, the function adds one temporary colour slot.
    # This is necessary for the arithmetic to maintain the no-overlap guarantee.
    # For odd moduli, edges with the same sum(endpoint, group) match, which we need to avoid.
    color_count = num_groups if num_groups % 2 else num_groups + 1

    # We then assign it a batch
    # pmod((18 + 19), 100) = 37
    batch_color = pmod(low_group + high_group, lit(color_count))

    grouped = (
        #  group = High + Low col with "--" separator
        df.withColumn(
            "group",
            concat(low_group.cast("string"), lit("--"), high_group.cast("string")),
        )
        # Assign batch via pmod(high+low, num_groups)
        .withColumn("batch", batch_color.cast("string"))
        # Store the batch_size
        .withMetadata("batch", {"neo4j_batch_size": batch_size})
    )

    schedule, counts = _rank_groups(grouped)
    return _guarantee_group_distinct_partitions(
        grouped, schedule, counts, df.sparkSession
    )


def _apply_repartitioning(
    spark_dataframe: DataFrame,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    staging_path: str | None = None,
) -> list[DataFrame]:
    return _repartition_batches(
        spark_dataframe, cache, direct=False, staging_path=staging_path
    )


def group_and_batch_spark_dataframe(
    spark_dataframe: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    batch_size: int = 20000,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    staging_path: str | None = None,
) -> list[DataFrame]:
    """Add a collision-safe execution schedule to a relationship DataFrame.

    Source and target identifiers are hashed into num_groups endpoint
    buckets. The unordered pair of endpoint buckets becomes the relationship's
    group, so reverse-direction edges receive the same group.

    Groups are assigned a batch using an edge-colouring formula. Different
    groups in one batch cannot share an endpoint bucket and are therefore safe
    to write concurrently. Batches must be written serially. For an even bucket
    count, one unused bucket is added so the same colouring rule remains safe.

    The returned batches contain the original columns plus group, batch, and
    groupKey. Each groupKey hashes to a distinct partition within its batch
    when using that batch's observed group count as the partition count.
    If bounded key search or its driver metadata budget is exhausted, that batch uses group instead and
    its groupKey values are null. Groups may then share partitions and some
    partitions may be empty; the collision-safe batch schedule is preserved.

    batch_size is stored as metadata for the Neo4j connector. Only per-batch
    counts and bounded candidate partition keys are collected on the driver.

    Without staging_path, preparation uses a temporary cache at the requested
    ``cache`` storage level, sorted by batch so Spark can skip decoding unrelated
    cached blocks. Returned batches also use ``cache``. Peak storage can include
    both the intermediate
    and completed batches. All batches are
    materialized before return unless ``cache=StorageLevel.NONE``, which permits
    recomputation during shipping. Large loads need sufficient executor disk,
    shuffle capacity, and an appropriate ``spark.sql.shuffle.partitions`` value.

    You can also use this to write the target Node's to Neo4j, before creating the relationships.
    For example, if you have a (:Person)-[:PEFORMED]-(:Action) relationships, you could use the below
    to ingest the nodes, and then the relationships:

    e.g.
    ```Python

    peopleDF = spark.createDataFrame(
        [
            {"name": "John", "surname": "Doe", "age": 42, sourceId: 123, targetId: 123456789, time: "2026-09-08T12:31:00"},
            {"name": "Jane", "surname": "Doe", "age": 40, sourceId: 987654321, targetId: 321, time: "2026-09-08T12:31:00"},
            **remainingData
        ]
    )

    # 1. Batch the dataframe

    batched = group_and_batch_spark_dataframe(peopleDf, source_col="idNumber", target_col="targetId", num_groups=100, batch_size=5000)

    # 2. Ingest the dataframe nodes only

    NODE_QUERY = '''
    Merge(p:Person {id: event.sourceId})
    Merge(a:Action {id: event.targetId})
    '''

    ingest_spark_dataframe(
        batched,
        save_mode="Overwrite",
        unpersist=False,
        options={
            "query": NODE_QUERY,
            "transaction.retries": 5,
            "transaction.retry.timeout": 2,
        },
    )

    # 3. Ingest the dataframe relationships

    REL_QUERY = '''
    Match(p:Person {id: event.sourceId})
    Match(a:Action {id: event.targetId})
    Merge(p)-[r:PERFORMED]-(a)
    SET r.time = event.time
    '''

    ingest_spark_dataframe(
        batched,
        save_mode="Overwrite",
        options={
            "query": REL_QUERY,
            "transaction.retries": 5,
            "transaction.retry.timeout": 2,
        },
    )
    ```

    Parameters
    ----------
    df : DataFrame
        Relationship rows to schedule.
    source_col : str
        Column containing each relationship's source identifier.
    target_col : str
        Column containing each relationship's target identifier.
    num_groups : int
        Number of endpoint hash buckets.
        Accepts an arbitrary number of groups.
    batch_size : int
        Neo4j transaction size saved in the batch column metadata.
    cache: { See ```pyspark.sql.DataFrame.persist``` and ```pyspark.StorageLevel```}
        Controls persistence of the returned batch DataFrames and, without
        staging_path, the temporary intermediate dataset.
        Defaults to ```StorageLevel.MEMORY_AND_DISK```, which uses disk and memory, with no replication.
        Use ```StorageLevel.NONE``` to disable both caches, if needed.
    staging_path : str or None, default None
        Optional new directory on storage shared by every executor. Writes
        intermediate Parquet (requiring Parquet-compatible input types)
        partitioned by batch so each batch reads only
        its own files, avoiding repeated disk-cache reads. Existing paths
        are rejected. Shipping with unpersist=True deletes this directory
        after releasing all batches, including on write failure. False
        retains caches and files for another pass or retry. Do not reuse
        staged batches after shipping has cleaned them up.

    Returns
    -------
    list[DataFrame]
        The input DataFrame with collision-safe group and batch columns, split and repartitioned per batch,
        and per batch-group, and persisted according to ```cache``` level.
    """
    if num_groups <= 0:
        raise ValueError("num_groups must be positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    grouped = _create_node_groupings_v2(
        df=spark_dataframe,
        source_col=source_col,
        target_col=target_col,
        num_groups=num_groups,
        batch_size=batch_size,
    )

    return _apply_repartitioning(grouped, cache=cache, staging_path=staging_path)
