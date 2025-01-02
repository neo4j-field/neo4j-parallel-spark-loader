from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct

from neo4j_parallel_spark_loader.predefined_components.grouping import (
    create_node_groupings,
)


def test_create_node_groupings(
    spark_fixture: SparkSession,
    predefined_components_grouping_data: List[Dict[str, int]],
) -> None:
    sdf = spark_fixture.createDataFrame(predefined_components_grouping_data)

    result: DataFrame = create_node_groupings(
        spark_dataframe=sdf,
        partition_col="partition_col",
        num_groups=4,
    )

    group_count = result.select(countDistinct("final_group")).collect()[0][0]

    assert group_count == 2
    assert "final_group" in result.columns
    assert result.filter(col("final_group").isNull()).count() == 0
