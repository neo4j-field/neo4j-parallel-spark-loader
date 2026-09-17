"""
```repartitionById``` was added in v4.1.0

Use where possible, if not fallback to calculated hashes in grouping_fallback

"""

import logging

from pyspark import StorageLevel
from pyspark.sql import DataFrame
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


def _create_node_groupings_v2(
    df: DataFrame,
    source_col: str,
    target_col: str,
    num_groups: int,
    batch_size: int,
) -> DataFrame:
    """Add a collision-safe execution schedule to a relationship DataFrame.

    Supports both monopartite graphs (endpoints in the same node set) and
    bipartite graphs (endpoints in separate node sets). The same schedule can
    also be used to create endpoint nodes before writing relationships.

    Endpoint identifiers are hashed into ``num_groups`` buckets. Each row's
    unordered pair of buckets becomes its ``group`` string, so reversing an
    edge does not change its group. Multiple rows can share a group.

    Groups are assigned to batches by adding their endpoint bucket numbers
    modulo an odd colour count. Distinct groups in a batch cannot share an
    endpoint bucket: given one endpoint and the batch, the other endpoint is
    uniquely determined modulo the colour count. Consequently, groups within
    one batch may be written concurrently; different batches must be written
    serially. This guarantee concerns locks on these endpoint nodes, not any
    additional shared entities that a custom write query might touch.

    Within each batch, the observed group strings are sorted and assigned
    consecutive integer ``groupKey`` values starting at zero. These are direct
    partition IDs, not hash keys. Use Spark 4.1+ ``repartitionById`` with the
    number of observed groups in that batch and the ``groupKey`` column.
    Ordinary ``repartition`` hashes the keys and can put distinct groups in
    the same partition. If groups are removed and the partition count changes,
    regenerate the keys for the remaining groups.

    Parameters
    ----------
    df : DataFrame
        Input rows containing the source and target identifier columns.
    source_col : str
        Column containing each relationship's source identifier.
    target_col : str
        Column containing each relationship's target identifier.
    num_groups : int
        Positive number of endpoint hash buckets. This is not the number of
        observed groups or the number of partitions in each batch.
    batch_size : int
        Positive Neo4j transaction size stored in the batch column metadata;
        it does not control the number of rows in a scheduled batch.

    Returns
    -------
    DataFrame
        Original columns plus ``group`` (string), ``batch`` (string), and
        ``groupKey`` (integer). The batch column carries ``neo4j_batch_size``
        metadata. Existing columns with these names are replaced.

    Notes
    -----
    Ranks distinct groups within each batch in Spark and collects only batch
    counts on the driver. Relationship rows and group identifiers stay
    distributed. This function does not repartition the returned rows; ``_apply_repartitioning`` performs that step.

    Examples
    --------
    Given input rows such as:

    >>> df = spark.createDataFrame(
    ...     [
    ...         {"name": "John", "surname": "Doe", "age": 42, "sourceId": 1, "targetId": 99, "time": "2026-09-08T12:31:00"},
    ...         {"name": "Jane", "surname": "Doe", "age": 40, "sourceId": 2, "targetId": 98, "time": "2026-09-08T12:31:00"},
    ...         {"name": "Joseph", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 3, "time": "2026-09-08T12:31:00"},
    ...         {"name": "Mary", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 4, "time": "2026-09-08T12:31:00"},
    ...     ]
    ... )

    With num_groups=100, the following walkthrough uses illustrative hash
    bucket values to show each intermediate step. These are the original
    example's assumed buckets, not asserted xxhash64 results for the IDs above.

    1. Hash the source and target identifiers into endpoint buckets:

    +-----------------------------+
    |pmod(xxhash64(sourceId), 100)|
    +-----------------------------+
    |                           18|
    |                           89|
    |                           25|
    |                           25|
    +-----------------------------+

    +-----------------------------+
    |pmod(xxhash64(targetId), 100)|
    +-----------------------------+
    |                           19|
    |                           25|
    |                            7|
    |                           27|
    +-----------------------------+

    2. Find the lower and higher bucket for each row. This ensures reverse
    edges receive the same group:

    +-------------------------------------------------------------------+
    |least(pmod(xxhash64(sourceId), 100), pmod(xxhash64(targetId), 100))|
    +-------------------------------------------------------------------+
    |                                                                 18|
    |                                                                 25|
    |                                                                  7|
    |                                                                 25|
    +-------------------------------------------------------------------+

    +----------------------------------------------------------------------+
    |greatest(pmod(xxhash64(sourceId), 100), pmod(xxhash64(targetId), 100))|
    +----------------------------------------------------------------------+
    |                                                                    19|
    |                                                                    89|
    |                                                                    25|
    |                                                                    27|
    +----------------------------------------------------------------------+

    3. Join the lower and higher bucket numbers with "--":

    +------+
    | group|
    +------+
    |18--19|
    |25--89|
    | 7--25|
    |25--27|
    +------+

    4. Assign the batch with pmod(low_group + high_group, 101). For 100
    endpoint buckets, the colour count is 101, not 100:

    +-----+
    |batch|
    +-----+
    |   37|
    |   13|
    |   32|
    |   52|
    +-----+

    For example, (18 + 19) % 101 = 37 and (25 + 89) % 101 = 13.
    The resulting rows retain their original properties:

    [
        {"name": "John", "surname": "Doe", "age": 42, "sourceId": 1, "targetId": 99, "time": "2026-09-08T12:31:00", "group": "18--19", "batch": "37", "groupKey": 0},
        {"name": "Jane", "surname": "Doe", "age": 40, "sourceId": 2, "targetId": 98, "time": "2026-09-08T12:31:00", "group": "25--89", "batch": "13", "groupKey": 0},
        {"name": "Joseph", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 3, "time": "2026-09-08T12:31:00", "group": "7--25", "batch": "32", "groupKey": 0},
        {"name": "Mary", "surname": "Doe", "age": 40, "sourceId": 98, "targetId": 4, "time": "2026-09-08T12:31:00", "group": "25--27", "batch": "52", "groupKey": 0},
    ]

    Each batch here contains only one observed group, so every groupKey is 0.
    The shared endpoint bucket 25 occurs in separate batches (13, 32, 52),
    which are written serially to prevent concurrent access to that bucket.

    5. To illustrate multiple groups in one batch and their groupKey values,
    Suppose num_groups=5 and the endpoint hashes produce these bucket pairs
    (the values below are buckets, not original node identifiers):

    source bucket  target bucket  group  batch  groupKey
    0              3              0--3   3      0
    1              2              1--2   3      1
    2              1              1--2   3      1
    4              4              4--4   3      2

    Batch 3 has three distinct groups with disjoint endpoint bucket sets.
    Repartitioning it with ``repartitionById(3, "groupKey")`` puts each group
    in its own partition. Reverse edges share a group and partition; the
    self-loop bucket group 4--4 is also isolated from the other groups.
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

    # Preserve the transaction-size hint for shipping.py to pass to Neo4j.
    grouped = (
        df.withColumn(
            "group",
            concat(low_group.cast("string"), lit("--"), high_group.cast("string")),
        )
        .withColumn("batch", batch_color.cast("string"))
        .withMetadata("batch", {"neo4j_batch_size": batch_size})
    )

    schedule, counts = _rank_groups(grouped)
    group_keys = schedule.select("group", F.col("ordinal").alias("groupKey"))
    return _join_group_keys(grouped, group_keys, sum(row["count"] for row in counts))


def _apply_repartitioning(
    spark_dataframe: DataFrame,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    staging_path: str | None = None,
) -> list[DataFrame]:
    return _repartition_batches(
        spark_dataframe, cache, direct=True, staging_path=staging_path
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
    groupKey. Each groupKey directly identifies a distinct partition within its batch
    when using that batch's observed group count as the partition count.

    batch_size is stored as metadata for the Neo4j connector. Only per-batch
    counts are collected on the driver. Requires Spark 4.1 or later.

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
        and per batch-group.
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
