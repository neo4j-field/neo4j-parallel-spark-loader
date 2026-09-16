# Using v2

Install via:

```bash
pip install "git+https://github.com/neo4j-field/neo4j-parallel-spark-loader.git@feat--fast-grouping-and-persistence"
```

v2 groups relationship rows so groups in the same batch do not share endpoint
buckets. Groups are written in parallel; batches are written sequentially. It
supports endpoints in the same node set or different node sets and automatically
selects the partitioning implementation for your Spark version.

Start with a Spark session configured with the Neo4j Spark connector, connection
URL, authentication, and database. The connector must match Spark's Scala version.

## Group and write

This example expects a DataFrame `df` with `sourceId` and `targetId` columns.

```python
from neo4j_parallel_spark_loader.v2 import (
    group_and_batch_spark_dataframe,
    ingest_spark_dataframe,
)

batches = group_and_batch_spark_dataframe(
    df,
    source_col="sourceId",
    target_col="targetId",
    num_groups=100,
    batch_size=20_000,
)

# Create endpoint nodes, retaining batches for the relationship pass.
ingest_spark_dataframe(
    batches,
    save_mode="Overwrite",
    options={"query": """
        MERGE (:Person {id: event.sourceId})
        MERGE (:Action {id: event.targetId})
    """},
    unpersist=False,
)

# Write relationships and release caches and any staging files.
ingest_spark_dataframe(
    batches,
    save_mode="Overwrite",
    options={"query": """
        MATCH (p:Person {id: event.sourceId})
        MATCH (a:Action {id: event.targetId})
        MERGE (p)-[:PERFORMED]->(a)
    """},
    unpersist=True,
)
```

If the nodes already exist, skip the first write. Other input columns are
available as `event.<column>` in your Cypher. Custom queries that modify additional
shared entities can still introduce lock contention.

## Options and lifecycle

- `num_groups` sets the number of endpoint hash buckets, not transaction size or
  total Spark partitions. Returned batches include `group`, `batch`, and `groupKey`.
- `batch_size` sets Neo4j's transaction size. An explicit `options["batch.size"]`
  overrides it. Other connector options can also be passed through `options`.
- `cache` defaults to `StorageLevel.MEMORY_AND_DISK`. Batches are materialized
  before grouping returns. Without caching, staged batches reread their Parquet
  files and reapply partitioning; they do not regroup the original input.
- `unpersist=True` is the shipping default: it releases caches and deletes
  loader-owned staging after all associated batches are released, including on
  write failure. `False` retains both for another pass or retry. If you abandon
  such a run, unpersist the batches and remove its staging directory yourself.
- `resume_from=3` starts at batch **3/N in the shipping logs**, using the same
  ordered batch list. Use idempotent queries when retrying. Once shipping has
  deleted staging, rebuild those batches before reusing them.

## Staging large loads

Optionally pass `staging_path="s3a://my-bucket/neo4j-staging/unique-run-id"` to
`group_and_batch_spark_dataframe`. Use a **new, dedicated prefix** accessible to
all executors, with the filesystem connector and read/write/list/delete access
configured. `s3://` also works on runtimes configured to support that scheme.

Staging writes Parquet partitioned by batch so each batch reads only its own
files. Existing paths are rejected, and input types must support Parquet.
Shipping with `unpersist=True` cleans up the staging path automatically.

Without staging, grouping uses a temporary disk-only cache. Either approach
needs sufficient disk and shuffle capacity; peak storage can include both the
intermediate dataset and prepared batches.

## Resume after restarting the process

If staging was retained, reopen it directly instead of calling grouping again:

```python
from neo4j_parallel_spark_loader.v2 import load_staged_batches

batches = load_staged_batches(spark, "s3a://my-bucket/neo4j-staging/unique-run-id")
ingest_spark_dataframe(
    batches,
    save_mode="Overwrite",
    options={"query": relationship_query},
    resume_from=3,  # Retry the batch reported as 3/N in the previous run.
    unpersist=False,  # Keep staging if this attempt fails or another pass is needed.
)
```

Staging includes resume metadata with the original schema, transaction size,
batch order, and partitioning strategy. Reloading creates lazy batch plans:
it does not read the source dataset, regroup rows, scan rows to rediscover the
schedule, or read skipped batches. Use the same write query and an idempotent
operation when retrying a partly written batch. The loader does not record which
writes completed; take `resume_from` from your previous shipping logs.

Use `unpersist=True` when staging should be deleted after the attempt, including
on failure. If it has already been deleted, it cannot be resumed. Reopening
requires staging created with resume metadata by this version; older staging
directories without that metadata cannot be reopened. Use each staging prefix
for one active run at a time. Direct partition IDs require Spark 4.1+ on reload.
