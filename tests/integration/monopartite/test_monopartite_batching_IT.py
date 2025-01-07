from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from neo4j_parallel_spark_loader.monopartite.batching import (
    color_complete_graph_with_self_loops,
    create_ingest_batches_from_groups,
)


def test_create_ingest_batches_from_groups_no_duplicate_group_rels(
    spark_fixture: SparkSession, monopartite_batching_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(
        monopartite_batching_data, ["source_group", "target_group"]
    )

    result: DataFrame = create_ingest_batches_from_groups(spark_dataframe=sdf)

    sources = result.select("source_group", "batch", "group").withColumnRenamed(
        "source_group", "source_or_target"
    )
    targets = result.select("target_group", "batch", "group").withColumnRenamed(
        "target_group", "source_or_target"
    )
    source_or_target = sources.union(targets).drop_duplicates()

    group_counts = source_or_target.groupBy("group", "batch").count()

    group_counts.show()
