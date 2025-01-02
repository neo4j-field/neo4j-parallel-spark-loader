from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from neo4j_parallel_spark_loader.bipartite.batching import create_ingest_batches_from_groups


def test_create_ingest_batches_from_groups(
    spark_fixture: SparkSession, bipartite_batching_data: List[Dict[str, int]]
) -> None:
    sdf: DataFrame = spark_fixture.createDataFrame(
        bipartite_batching_data, ["source_group", "target_group"]
    )

    num_colors = 4

    result = create_ingest_batches_from_groups(spark_dataframe=sdf)

    assert result.agg({"batch": "max"}).first()[0] + 1 <= num_colors
