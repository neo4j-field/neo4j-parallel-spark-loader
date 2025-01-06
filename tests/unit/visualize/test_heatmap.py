from pyspark.sql import DataFrame

from neo4j_parallel_spark_loader.visualize.heatmap import (
    _format_spark_dataframe_for_visualization,
)


def test_create_heatmap_dataframe(heatmap_spark_dataframe: DataFrame) -> None:
    data = _format_spark_dataframe_for_visualization(
        spark_dataframe=heatmap_spark_dataframe
    )

    assert heatmap_spark_dataframe.count() == len(data)
    assert set(heatmap_spark_dataframe.columns) == set(data[0].keys())
