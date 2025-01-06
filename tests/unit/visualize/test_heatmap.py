from pyspark.sql import DataFrame

from neo4j_parallel_spark_loader.visualize.heatmap import create_heatmap_dataframe


def test_create_heatmap_dataframe(heatmap_spark_dataframe: DataFrame) -> None:
    df = create_heatmap_dataframe(spark_dataframe=heatmap_spark_dataframe)

    assert heatmap_spark_dataframe.count() == df.shape[0]
    assert set(heatmap_spark_dataframe.columns) == set(df.columns)
