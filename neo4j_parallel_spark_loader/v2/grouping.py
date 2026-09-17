import logging

from pyspark import StorageLevel
from pyspark.sql import DataFrame

logging.basicConfig(
    force=True,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
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

    The returned batches contain the original columns plus ```group```, ```batch```, and
    ```groupKey```. Each ```groupKey``` hashes to a distinct partition within its ```batch```
    when using that batch's observed ```group``` count as the partition count.
    On older Spark versions, exhausted partition-key searches or metadata
    budgets fall back to partitioning affected batches by ```group```, with null
    ```groupKey``` values. This preserves the ```batch``` schedule but may leave some
    partitions empty.

    ```batch_size``` is stored as metadata for the Neo4j connector. Only per-batch
    counts and bounded candidate partition keys are collected on the driver.

    Without ```staging_path```, preparation uses a temporary cache at the requested
    ``cache`` storage level, sorted by batch so Spark can skip decoding unrelated
    cached blocks. Returned batches also use ``cache``. Peak storage can include
    both the intermediate and completed batches. All batches are
    materialized before return unless ``cache=StorageLevel.NONE``, which permits
    recomputation during shipping. Large loads need sufficient executor disk,
    shuffle capacity, and an appropriate ``spark.sql.shuffle.partitions`` value.

    You can also use this to write the target Node's to Neo4j, before creating the relationships.
    For example, if you have a ```(:Person)```-```[:PEFORMED]```-```(:Action)``` relationships, you could use the below
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
        Adjusting the group size based on data cardinality is recommended.
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
        A list of collision-safe DataFrame batches, calculated from the input DataFrame.
        Each DataFrame is repartitioned from the source, and each group is repartitioned within per batch-group.
    """
    if num_groups <= 0:
        raise ValueError("num_groups must be positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    spark_version = spark_dataframe.sparkSession.version

    if tuple(int(part) for part in spark_version.split(".")[:2]) >= (4, 1):
        from ._grouping_4_1_0 import _apply_repartitioning, _create_node_groupings_v2

        grouped = _create_node_groupings_v2(
            df=spark_dataframe,
            source_col=source_col,
            target_col=target_col,
            num_groups=num_groups,
            batch_size=batch_size,
        )

        return _apply_repartitioning(grouped, cache=cache, staging_path=staging_path)
    else:
        from ._grouping_fallback import _apply_repartitioning, _create_node_groupings_v2

        grouped = _create_node_groupings_v2(
            df=spark_dataframe,
            source_col=source_col,
            target_col=target_col,
            num_groups=num_groups,
            batch_size=batch_size,
        )

        return _apply_repartitioning(grouped, cache=cache, staging_path=staging_path)
