from typing import List, Literal, Optional, Union

from pyspark.sql import DataFrame

from ..bipartite import (
    group_and_batch_spark_dataframe as group_and_batch_bipartite,
)
from ..predefined_components import (
    group_and_batch_spark_dataframe as group_and_batch_predefined,
)
from .ingest import ingest_spark_dataframe


def build_relationship(
    df: DataFrame,
    relationship_name: str,
    source_labels: str,
    source_keys: str,
    target_labels: str,
    target_keys: str,
    rel_props: List[str] = None,
    group_keys: List[str] = None,
    num_groups: Optional[int] = 10,
    max_serial: Optional[int] = 1000000,
    strategy: Literal["greedy", "hash"] = "greedy",
    on_null_batch: Literal["raise", "skip"] = "raise",
    checkpoint_path: Optional[str] = None,
    sort_columns: Optional[List[str]] = None,
    materialize: Union[None, Literal["persist"], str] = None,
) -> None:
    """Build a relationship between two nodes.
    Params:
        rel_props: List[str]
            list of df columns to use as rel properties
        group_keys: List[str]
            list of df columns to use as group keys for parallel processing.
            1 key uses predefined batching, 2 uses bipartite batching
        num_groups: Optional[int] , optional
            The number of partitions to split Spark DataFrame into. By default 10
        max_serial: Optional[int] , optional
            The maximum number of relationships to process serially.
            Any number of rows above this number will be processed in parallel
        strategy: Literal["greedy", "hash"], optional
            The grouping strategy to use. "greedy" balances group sizes but collects
            distinct id counts to the driver and does not scale to very large datasets.
            "hash" computes group assignments entirely in Spark and scales to very large
            datasets, at the cost of not balancing group sizes. By default "greedy"
        on_null_batch: Literal["raise", "skip"], optional
            Passed to `ingest_spark_dataframe`. "raise" stops before writing if any rows
            have a null node id; "skip" warns and ingests the rest. By default "raise"
        checkpoint_path: Optional[str], optional
            Passed to `ingest_spark_dataframe`. When set, the grouped DataFrame is written
            once as Parquet partitioned by batch and each batch is read back from there
            instead of recomputing the input. The checkpoint can be used to resume a failed
            load. To avoid recomputing the input at all, use `materialize` instead.
            By default None
        sort_columns: Optional[List[str]], optional
            Passed to `ingest_spark_dataframe`. Sorts each group's rows by these columns
            before writing, typically the source node id, to improve write locality on
            Neo4j. By default None
        materialize: Union[None, Literal["persist"], str], optional
            Materialize `df` before anything else runs, so its lineage is computed exactly
            once instead of once for grouping and once per batch. Recommended when `df` is
            expensive to compute (joins, aggregations, `distinct`, UDFs) or not
            deterministic.
            "persist" caches `df` with `DataFrame.persist()`: the grouping pass fills the
            cache and every later pass reads it. The cache is released when this function
            returns. Partitions lost with an executor are recomputed from the lineage.
            Not available on Databricks serverless compute.
            Any other string is a path: `df` is written there once as Parquet and read back.
            Existing data at the path is overwritten, and the caller is responsible for
            deleting it after the load. Column names must be valid Parquet column names.
            By default None (no materialization)
    """
    options = {
        "relationship": relationship_name,
        "relationship.save.strategy": "keys",
        "relationship.source.save.mode": "Match",
        "relationship.source.labels": source_labels,
        "relationship.source.node.keys": source_keys,
        "relationship.target.save.mode": "Match",
        "relationship.target.labels": target_labels,
        "relationship.target.node.keys": target_keys,
    }
    if rel_props:
        options["relationship.properties"] = ",".join(rel_props)

    unpersist = None
    if materialize == "persist":
        print("Persisting input")
        df = df.persist()
        unpersist = df.unpersist
    elif materialize is not None:
        assert (
            isinstance(materialize, str) and materialize
        ), '`materialize` must be None, "persist", or a path'
        print(f"Materializing input to {materialize}")
        df.write.mode("overwrite").parquet(materialize)
        df = df.sparkSession.read.parquet(materialize)

    try:
        if group_keys:
            # grouping counts the rows and builds the ingest plan in the same pass, so neither
            # this function nor ingest needs a pass of its own
            if len(group_keys) == 1:
                grouping_name = "Predefined"
                batched_df, plan = group_and_batch_predefined(
                    df, group_keys[0], num_groups, strategy=strategy, return_plan=True
                )
            else:
                grouping_name = "Bipartite"
                batched_df, plan = group_and_batch_bipartite(
                    df,
                    group_keys[0],
                    group_keys[1],
                    num_groups,
                    strategy=strategy,
                    return_plan=True,
                )
            row_count = plan.total_rows
        else:
            row_count = df.count()

        print(f"""Building {row_count} relationships""")
        if group_keys and row_count > max_serial:
            print("Building in parallel")
            print(f"Using {grouping_name} Grouping")
            ingest_spark_dataframe(
                spark_dataframe=batched_df,
                save_mode="Overwrite",
                options=options,
                on_null_batch=on_null_batch,
                checkpoint_path=checkpoint_path,
                sort_columns=sort_columns,
                plan=plan,
            )
        else:
            print("Building in series")
            df = (
                df.coalesce(1)
                .write.format("org.neo4j.spark.DataSource")
                .mode("Overwrite")
                .options(**options)
                .save()
            )
    finally:
        if unpersist is not None:
            unpersist()
