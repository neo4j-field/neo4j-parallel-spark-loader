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
    Collects the distinct batch/group schedule on the driver to assign keys.
    Relationship rows remain distributed. This function does not repartition
    the returned rows; ``_apply_repartitioning`` performs that step.

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

    # Only observed groups need partitions; collect their identifiers, not rows.
    schedule = (
        grouped.select("batch", "group")
        .distinct()
        .groupBy("batch")
        .agg(sort_array(collect_list("group")).alias("groups"))
        .collect()
    )

    # Each group belongs to exactly one batch, so group alone is a valid key.
    key_rows = [
        (group, key) for entry in schedule for key, group in enumerate(entry["groups"])
    ]
    group_keys = df.sparkSession.createDataFrame(key_rows, "group string, groupKey int")
    return grouped.drop("groupKey").join(group_keys, on="group", how="left")


def _apply_repartitioning(
    spark_dataframe: DataFrame,
    cache: StorageLevel = StorageLevel.MEMORY_AND_DISK,
) -> list[DataFrame]:
    logging.info(f"Repartitioning and persisting the DF using strategy: {cache!s}")

    scheduled_df = spark_dataframe.repartition("batch").persist(cache)

    logging.info("Finished repartitioning the full dataset on the `batch` key.")

    logging.info("Generating the schedule for the dataset.")
    try:
        schedule: list[Row] = (
            scheduled_df.select("batch", "group")
            .distinct()
            .groupBy("batch")
            .agg(sort_array(collect_list("group")).alias("groups"))
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
        logging.info(
            "Finished generating the schedule for the dataset. Set logging to DEBUG to log the schedule."
        )
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
                .repartitionById(
                    partition_count,
                    col("groupKey"),
                )
                .persist(cache)
            )
            batches.append(partition)
            partition.count()  # Force computation now, so that each batch is cached here, instead of computed at shipping
            completed += 1
            logging.info(
                f"Finished repartitioning {completed}/{total_batches}, with {len(entry['groups'])} groups"
            )

        logging.info("Finished applying per-batch repartitioning.")

        return batches
    except Exception:
        for batch in batches:
            batch.unpersist(
                blocking=False
            )  # Ensure any batch creation failure unpersists all already created batches
        raise
    finally:
        scheduled_df.unpersist(blocking=False)


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
    groupKey. Each groupKey directly identifies a distinct partition within its batch
    when using that batch's observed group count as the partition count.

    batch_size is stored as metadata for the Neo4j connector. Only schedule
    identifiers are collected on the driver. Requires Spark 4.1 or later.

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
        Accepts an arbitrary number of groups.
    batch_size : int
        Neo4j transaction size saved in the batch column metadata.
    cache: { See ```pyspark.sql.DataFrame.persist``` and ```pyspark.StorageLevel```}
        Controls the level of persistence, and therefore caching, for the partitioned and grouped ```DataFrame```.
        If not persisted, the ```DataFrame``` will be recomputed at shipping time, which is not recommended.
        Defaults to ```StorageLevel.MEMORY_AND_DISK```, which uses disk and memory, with no replication.
        Use ```StorageLevel.NONE``` to disabling caching the partitions, if needed.

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

    return _apply_repartitioning(grouped, cache=cache)
