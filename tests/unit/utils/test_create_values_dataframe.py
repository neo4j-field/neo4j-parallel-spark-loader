from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from parallel_spark_loader.utils.grouping import create_value_counts_dataframe


def test_create_value_counts_dataframe(
    spark_fixture: SparkSession,
    utils_data: List[Dict[str, int]],
) -> None:
    sdf = spark_fixture.createDataFrame(utils_data)
    result: DataFrame = create_value_counts_dataframe(
        spark_dataframe=sdf, grouping_column="partition_col"
    )

    result_dict = {}
    list_of_dicts = result.collect()
    [
        result_dict.update(
            {row.asDict().get("partition_col"): row.asDict().get("count")}
        )
        for row in list_of_dicts
    ]

    assert result_dict.get("a") == 3
    assert result_dict.get("b") == 2
