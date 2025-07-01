from typing import List, Optional

from pyspark.sql import DataFrame

from neo4j_parallel_spark_loader import ingest_spark_dataframe
from neo4j_parallel_spark_loader.bipartite import (
    group_and_batch_spark_dataframe as group_and_batch_bipartite,
)
from neo4j_parallel_spark_loader.predefined_components import (
    group_and_batch_spark_dataframe as group_and_batch_predefined,
)


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

    print(f"""Building {df.count()} relationships""")
    if group_keys and len(group_keys) > 0 and df.count() > max_serial:
        print("Building in parallel")
        if len(group_keys) == 1:
            print("Using Predefined Grouping")
            batched_df = group_and_batch_predefined(df, group_keys[0], num_groups)
        else:
            print("Using Bipartite Grouping")
            batched_df = group_and_batch_bipartite(
                df, group_keys[0], group_keys[1], num_groups
            )

        ingest_spark_dataframe(
            spark_dataframe=batched_df,
            save_mode="Overwrite",
            options=options,
            num_groups=num_groups,
        )
    else:
        print("Building in series")
        df = (
            df.write.format("org.neo4j.spark.DataSource")
            .mode("Overwrite")
            .options(**options)
            .save()
        )
