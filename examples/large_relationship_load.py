"""
Load a very large relationship DataFrame (hundreds of millions of rows) into Neo4j
with neo4j-parallel-spark-loader.

This example encodes the configuration recommended after the 110M-row Aura load:

* hash grouping, so grouping does no driver-side work regardless of distinct id count
* MERGE (the connector's Overwrite save mode), so a replayed task or batch is harmless
* a per-transaction timeout set through the connector, so an orphaned transaction
  releases its locks in minutes rather than holding them for the length of Aura's
  fixed timeout
* no speculative execution and one Spark task attempt, so a stall becomes a visible
  failure of one batch; connector-level retries stay on because MERGE makes a re-sent
  transaction harmless and the server rolls back a dropped connection's work at once
* pooled connections rotated every three minutes, since resets were observed to hit
  connections a few minutes into their life
* a Parquet checkpoint partitioned by batch, so each batch reads only its own files
  and a failed batch can be replayed on its own
* rows sorted by source id within each group, so consecutive relationships in a
  transaction touch the same node record and chain and each transaction dirties far
  fewer pages, which keeps Neo4j checkpoints short under a sustained write load

Fill in the CONFIGURATION block, then run:

    spark-submit --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.1_for_spark_3 \
        examples/large_relationship_load.py

To replay a failed batch, run with `--resume-from <batch>` and the same checkpoint path.
"""

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from neo4j_parallel_spark_loader import ingest_spark_dataframe
from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe

# --------------------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------------------

NEO4J_URL = "neo4j+s://xxxxxxxx.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "..."  # read from a secret store in real use
NEO4J_DATABASE = "neo4j"

# Where the relationship rows come from. Any DataFrame works; a Parquet or Delta path is
# typical. It must contain the two id columns and the relationship property columns.
SOURCE_PATH = "s3://your-bucket/relationships/account_has_device/"

SOURCE_ID_COL = "accountId"
TARGET_ID_COL = "bfpStatefulDeviceId"
RELATIONSHIP_PROPERTY_COLS = ["firstSeenDt", "graphHash"]

# Durable storage the cluster can write to. The grouped DataFrame is written here once,
# partitioned by batch. Keep it until the load has finished, then delete it.
CHECKPOINT_PATH = "s3://your-bucket/tmp/account_has_device_load/"

# Number of relationship-write transactions Neo4j will run concurrently. For bipartite
# data this is also the number of batches. Keep it at or below the Aura instance's CPU
# count; more writers than cores queue up in the server and stretch every transaction.
# With 500M rows and 16 groups each batch is ~31M rows split into 16 groups of ~2M.
NUM_GROUPS = 16

# Rows per Neo4j transaction. Smaller transactions hold locks for less time and shrink
# the window in which a lost connection leaves the commit outcome unknown.
BATCH_SIZE = 10_000

# Per-transaction timeout in milliseconds, enforced by the server regardless of Aura's
# fixed db.transaction.timeout. A healthy 10k-row MERGE transaction takes seconds. Set
# this several times longer than the slowest healthy transaction you observe and far
# shorter than the stall you want to prevent. 5 minutes is a reasonable start.
TRANSACTION_TIMEOUT_MS = 5 * 60 * 1000

# --------------------------------------------------------------------------------------


def build_spark() -> SparkSession:
    """
    Speculation and task retries are SparkContext-level settings, so they must be set
    when the session is created. On a shared cluster where they cannot be changed, MERGE
    keeps the default of 4 task attempts tolerable; it just wastes time on a stall.
    """
    return (
        SparkSession.builder.appName("account_has_device relationship load")
        # A speculative twin of a slow task writes the same rows and fights it for locks.
        .config("spark.speculation", "false")
        # A relaunched task re-sends rows its first attempt may already have committed.
        # Fail the batch instead and replay it deliberately.
        .config("spark.task.maxFailures", "1")
        .config("neo4j.url", NEO4J_URL)
        .config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", NEO4J_USER)
        .config("neo4j.authentication.basic.password", NEO4J_PASSWORD)
        .config("neo4j.database", NEO4J_DATABASE)
        .getOrCreate()
    )


def neo4j_write_options() -> dict:
    """
    Connector options. The relationship is written with the connector's native `keys`
    strategy in Overwrite mode, which the connector turns into MERGE, as its
    documentation recommends. Node ends are matched, not created, so the node key
    constraints must already exist.
    """
    return {
        "relationship": "HAS_BFP_STATEFUL_DEVICE",
        "relationship.save.strategy": "keys",
        "relationship.source.save.mode": "Match",
        "relationship.source.labels": ":Account",
        "relationship.source.node.keys": f"{SOURCE_ID_COL}:accountId",
        "relationship.target.save.mode": "Match",
        "relationship.target.labels": ":BfpStatefulDevice",
        "relationship.target.node.keys": f"{TARGET_ID_COL}:bfpStatefulDeviceId",
        "relationship.properties": ",".join(RELATIONSHIP_PROPERTY_COLS),
        # --- transaction behaviour -------------------------------------------------
        "batch.size": str(BATCH_SIZE),
        # Server-side termination of any transaction from this load that runs too long.
        # This is what frees locks held by a transaction whose client has gone away.
        "db.transaction.timeout": str(TRANSACTION_TIMEOUT_MS),
        # Re-send a failed 10k-row transaction on a new connection after a short pause.
        # Safe because MERGE is idempotent; observed connection resets cost seconds
        # this way instead of a failed batch.
        "transaction.retries": "5",
        "transaction.retry.timeout": "5000",
        # Rotate pooled connections before the age at which resets were observed.
        "connection.max.lifetime.msecs": "180000",
    }


def prepare(df: DataFrame) -> DataFrame:
    """
    Hash grouping hashes the typed value, so both id columns must share a type. Casting
    to string is the safe choice. Rows with a null id cannot become relationships and
    would stop the ingest with an error, so drop them here where the count can be logged.
    """
    df = df.withColumn(SOURCE_ID_COL, col(SOURCE_ID_COL).cast("string")).withColumn(
        TARGET_ID_COL, col(TARGET_ID_COL).cast("string")
    )
    keep = df.filter(col(SOURCE_ID_COL).isNotNull() & col(TARGET_ID_COL).isNotNull())
    return keep.select(SOURCE_ID_COL, TARGET_ID_COL, *RELATIONSHIP_PROPERTY_COLS)


def full_load(spark: SparkSession) -> None:
    df = prepare(spark.read.parquet(SOURCE_PATH))

    # Adds `group` and `batch` columns. Hash strategy: no count, no collect, no join.
    grouped = group_and_batch_spark_dataframe(
        df, SOURCE_ID_COL, TARGET_ID_COL, NUM_GROUPS, strategy="hash"
    )

    # Writes the grouped frame once to Parquet partitioned by batch, reads it back, and
    # writes batch by batch. Every group in a batch gets its own Spark partition.
    ingest_spark_dataframe(
        grouped,
        "Overwrite",
        neo4j_write_options(),
        checkpoint_path=CHECKPOINT_PATH,
        sort_columns=[SOURCE_ID_COL],
    )


def resume_from(spark: SparkSession, first_batch: int) -> None:
    """
    Replay batches from the checkpoint without rewriting it. MERGE makes re-running a
    partially loaded batch harmless. Before calling this, run SHOW TRANSACTIONS on the
    database and terminate anything still running from the failed batch; with the
    transaction timeout in place there should be nothing to terminate.
    """
    remaining = spark.read.parquet(CHECKPOINT_PATH).filter(col("batch") >= first_batch)
    ingest_spark_dataframe(
        remaining, "Overwrite", neo4j_write_options(), sort_columns=[SOURCE_ID_COL]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--resume-from",
        type=int,
        metavar="BATCH",
        help="replay batches >= BATCH from the existing checkpoint instead of loading",
    )
    args = parser.parse_args()

    # The library logs one line per batch at INFO with group and row counts.
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("neo4j_parallel_spark_loader").setLevel(logging.INFO)

    spark = build_spark()

    # Fail early rather than discover a speculative twin halfway through the load.
    if spark.conf.get("spark.speculation", "false").lower() == "true":
        logging.error("spark.speculation is enabled; refusing to write to Neo4j")
        return 2

    if args.resume_from is None:
        full_load(spark)
    else:
        resume_from(spark, args.resume_from)
    return 0


if __name__ == "__main__":
    sys.exit(main())
