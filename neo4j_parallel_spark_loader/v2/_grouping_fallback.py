"""
```repartitionById``` was added in v4.1.0

Use calculated hashes as a fallback

"""

import logging
import math

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    collect_list,
    concat,
    greatest,
    least,
    lit,
    pmod,
    sort_array,
    xxhash64,
)
from pyspark.sql.types import Row

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
    initial_candidate_count = max(32, math.ceil(partition_count * (math.log(partition_count) + 16)))
    max_attempts = 8
    for attempt in range(max_attempts):
        candidate_count = initial_candidate_count * (2**attempt)
        keys = spark.range(candidate_count).select("id", pmod(F.hash("id"), lit(partition_count)).alias("partition")).groupBy("partition").agg(F.min("id").alias("key")).collect()
        if len(keys) == partition_count:
            return [row["key"] for row in sorted(keys, key=lambda row: row["partition"])]
    return None


def _guarantee_group_distinct_partitions(grouped: DataFrame, schedule: list[Row], session):
    """
    Generate collision-free long keys where possible. Leave keys null for
    batches whose search was exhausted, marking them for partitioning by group.
    """
    keys_by_count: dict[int, list[int] | None] = {}
    expressions = []
    for entry in schedule:
        count = len(entry["groups"])
        if count not in keys_by_count:
            keys_by_count[count] = _partition_keys(session, count)
        keys = keys_by_count[count]
        if keys is None:
            continue
        for group, key in zip(entry["groups"], keys):
            expressions.extend((lit(group), lit(key).cast("long")))
    group_keys = F.create_map(*expressions).cast("map<string,bigint>")
    return grouped.withColumn("groupKey", group_keys[col("group")])


def _create_node_groupings_v2(
    df: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    batch_size: int,
) -> DataFrame:
    """Add a collision-safe execution schedule to a relationship DataFrame.

    Adds group, batch, and a long groupKey. Keys are chosen for the observed
    group count in each batch; repartition using that count and groupKey.
    Filtering out whole groups requires regenerating keys before changing
    the partition count. Only schedule identifiers are collected from df.
    If key search is exhausted, groupKey is null for that batch and
    _apply_repartitioning uses the group column instead.

    This approach works for Nodes, and both Monopartite and Bipartite Datasets.

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
    batch_size : int
        Neo4j transaction size saved in the batch column metadata.

    Returns
    -------
    DataFrame
        The input DataFrame with collision-safe group and batch columns.

    Examples
    --------
    Given:

    >>> DataFrame(
    ...     [
    ...         { "name": "John", "surname": "Doe", "age": 42, "sourceId": 1, "targetId": 99, "time": "2026-09-08T12:31:00",},
    ...         { "name": "Jane", "surname": "Doe", "age": 40, "sourceId": 2, "targetId": 98, "time": "2026-09-08T12:31:00",},
    ...         {"name": "Jospeh", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 3, "time": "2026-09-08T12:31:00"},
    ...         { "name": "Mary", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 4, "time": "2026-09-08T12:31:00",},
    ...     ]
    ... )

    With num_groups = 100

    Produce:
    >>> DataFrame(
    ...     [
    ...         { "name": "John", "surname": "Doe", "age": 42, "sourceId": 1, "targetId": 99, "time": "2026-09-08T12:31:00", group: "18--19", "batch": 37},
    ...         { "name": "Jane", "surname": "Doe", "age": 40, "sourceId": 2, "targetId": 98, "time": "2026-09-08T12:31:00", group: "25--89", "batch": 13},
    ...         { "name": "Joseph", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 3, "time": "2026-09-08T12:31:00", group: "7--25",  "batch": 32},
    ...         { "name": "Mary", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 4, "time": "2026-09-08T12:31:00", group: "25--27", "batch": 52},
    ...     ]
    ... )

    >>>
    ...+-----------------------------+
    ...|pmod(xxhash64(sourceId), 100)|
    ...+-----------------------------+
    ...|                           18|
    ...|                           89|
    ...|                           25|
    ...|                           25|
    ...+-----------------------------+
    ...
    ...
    ...+-----------------------------+
    ...|pmod(xxhash64(targetId), 100)|
    ...+-----------------------------+
    ...|                           19|
    ...|                           25|
    ...|                            7|
    ...|                           27|
    ...+-----------------------------+
    ...
    ...+-------------------------------------------------------------------+
    ...|least(pmod(xxhash64(sourceId), 100), pmod(xxhash64(targetId), 100))|
    ...+-------------------------------------------------------------------+
    ...|                                                                 18|
    ...|                                                                 25|
    ...|                                                                  7|
    ...|                                                                 25|
    ...+-------------------------------------------------------------------+
    ...
    ...+----------------------------------------------------------------------+
    ...|greatest(pmod(xxhash64(sourceId), 100), pmod(xxhash64(targetId), 100))|
    ...+----------------------------------------------------------------------+
    ...|                                                                    19|
    ...|                                                                    89|
    ...|                                                                    25|
    ...|                                                                    27|
    ...+----------------------------------------------------------------------+
    ...
    ...+------+
    ...| group|
    ...+------+
    ...|18--19|
    ...|25--89|
    ...| 7--25|
    ...|25--27|
    ...+------+
    ...
    ...+-----+
    ...|batch|
    ...+-----+
    ...|   37|
    ...|   13|
    ...|   32|
    ...|   52|
    ...+-----+

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
        #  group = High + Low col with "--" seperator
        df.withColumn(
            "group",
            concat(low_group.cast("string"), lit("--"), high_group.cast("string")),
        )
        # Assign batch via pmod(high+low, num_groups)
        .withColumn("batch", batch_color.cast("string"))
        # Store the batch_size
        .withMetadata("batch", {"neo4j_batch_size": batch_size})
    )

    # Keys depend on the number of groups actually present in each batch.
    # Collect only the schedule, never the relationship rows.
    schedule = grouped.select("batch", "group").distinct().groupBy("batch").agg(sort_array(collect_list("group")).alias("groups")).collect()

    return _guarantee_group_distinct_partitions(grouped=grouped, schedule=schedule, session=df.sparkSession)


def _apply_repartitioning(
    spark_dataframe: DataFrame,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
) -> list[DataFrame]:
    logging.info(f"Repartitioning and persisting the DF using strategy: {cache!s}")

    scheduled_df = spark_dataframe.repartition("batch").persist(cache)

    logging.info("Finished repartitioning the full dataset on the `batch` key.")

    logging.info("Generating the schedule for the dataset.")
    schedule: list[Row] = (
        scheduled_df.select("batch", "group", "groupKey")
        .distinct()
        .groupBy("batch")
        .agg(
            sort_array(collect_list("group")).alias("groups"),
            F.max(col("groupKey").isNull().cast("int")).alias("use_group"),
        )
        .orderBy("batch")
        .collect()
    )
    """
    Example Schedule
    ----------
    Row(batch='0', groups=['0-
      -0', '10--91', '11--90', '12--89', '13--88', '14--87', '15--86', '16--85', '17--84', '18--83', '19--82', '2--99', '20--81',
      '21--80', '22--79', '23--78', '24--77', '25--76', '26--75', '27--74', '28--73', '29--72', '3--98', '30--71', '31--70', '32--
      69', '33--68', '34--67', '35--66', '36--65', '37--64', '38--63', '39--62', '4--97', '40--61', '41--60', '42--59', '43--58',
      '44--57', '45--56', '46--55', '47--54', '48--53', '49--52', '5--96', '50--51', '6--95', '7--94', '8--93', '9--92']),

    Row(batch='1', groups=['0--1', '10--92', '11--91', '12--90', '13--89', '14--88', '15--87', '16--86', '17--85', '18--84',
      '19--83', '20--82', '21--81', '22--80', '23--79', '24--78', '25--77', '26--76', '27--75', '28--74', '29--73', '3--99', '30--
      72', '31--71', '32--70', '33--69', '34--68', '35--67', '36--66', '37--65', '38--64', '39--63', '4--98', '40--62', '41--61',
      '42--60', '43--59', '44--58', '45--57', '46--56', '47--55', '48--54', '49--53', '5--97', '50--52', '51--51', '6--96', '7--95',
      '8--94', '9--93']), 
    """
    logging.info("Finished generating the schedule for the dataset. Set logging to DEBUG to log the schedule.")
    logging.debug(schedule)

    logging.info(f"Applying per-batch repartitioning, using strategy {cache!s}")
    total_batches = len(schedule)
    completed = 0
    batches = []
    for entry in schedule:
        logging.debug(f"Batch has {len(entry['groups'])} groups")
        partition_count = len(entry["groups"])
        partition = (
            scheduled_df.filter(col("batch") == entry["batch"])
            .repartition(
                partition_count,
                col("group" if entry["use_group"] else "groupKey"),
            )
            .persist(cache)
        )
        batches.append(partition)
        completed += 1
        logging.info(f"Finished repartitioning {completed}/{total_batches}, with {len(entry['groups'])} groups")

    logging.info("Finished applying per-batch repartitioning.")

    return batches


def group_and_batch_spark_dataframe(
    spark_dataframe: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    batch_size: int = 20000,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
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
    If bounded key search is exhausted, that batch uses group instead and
    its groupKey values are null. Groups may then share partitions and some
    partitions may be empty; the collision-safe batch schedule is preserved.

    batch_size is stored as metadata for the Neo4j connector. Only schedule
    identifiers and candidate partition keys are collected on the driver.

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
    batch_size : int
        Neo4j transaction size saved in the batch column metadata.
    cache: { See ```pyspark.sql.DataFrame.persist``` and ```pyspark.StorageLevel```}
        Controls the level of persistence, and therefore caching, for the partitioned and grouped dataframe.
        If not persisted, the DataFrame will be recomputed at shipping time, which is not recommended.
        Defaults to ```StorageLevel.MEMORY_AND_DISK```, which uses disk and memory, with no replication.
        Use ```StorageLevel.NONE``` to disabling caching the partitions, if needed.

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

    return _apply_repartitioning(grouped, cache=cache)
