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
    sdf = spark_fixture.createDataFrame(monopartite_batching_data)

    result: DataFrame = create_ingest_batches_from_groups(spark_dataframe=sdf)

    sources = result.select("batch", "group").withColumn("source_or_target", col("group").substr(1, 1))
    targets = result.select("batch", "group").withColumn("source_or_target", col("group").substr(-1, 1))
    source_or_target = sources.unionAll(targets)

    group_counts = source_or_target.groupBy("group", "batch").count()

    group_counts.show()
