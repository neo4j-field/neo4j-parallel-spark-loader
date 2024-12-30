from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from parallel_spark_loader.predefined_components.batching import (
    create_ingest_batches_from_groups,
)


def test_create_ingest_batches_from_groups(
    spark_fixture: SparkSession,
    predefined_components_batching_data: List[Dict[str, int]],
) -> None:
    sdf: DataFrame = spark_fixture.createDataFrame(
        predefined_components_batching_data,
        ["source_group", "target_group", "partition_col"],
    )

    result = create_ingest_batches_from_groups(spark_dataframe=sdf)

    counts_sdf = result.groupBy("batch").count()

    assert counts_sdf.select("count").first()[0] == 5
    assert result.select("batch").first()[0] == 0
