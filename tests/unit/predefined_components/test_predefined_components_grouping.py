from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, countDistinct, expr

from neo4j_parallel_spark_loader.predefined_components import (
    group_and_batch_spark_dataframe,
)
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

    group_count = result.select(countDistinct("group")).collect()[0][0]

    assert group_count == 2
    assert "group" in result.columns
    assert result.filter(col("group").isNull()).count() == 0


def test_greedy_groups_invalid_utf8_partition_values(
    spark_fixture: SparkSession,
) -> None:
    # 0xC3 followed by "(" is not valid UTF-8 and used to lose its group on the driver round trip
    sdf = spark_fixture.range(20).select(
        expr(
            "CASE WHEN id % 4 = 0 THEN cast(unhex('C328') AS STRING) "
            "ELSE cast(id % 3 AS STRING) END"
        ).alias("partition_col")
    )

    result = group_and_batch_spark_dataframe(sdf, "partition_col", num_groups=4)

    assert result.filter(col("group").isNull()).count() == 0
    assert result.select("partition_col", "group").distinct().count() == 4


def test_greedy_groups_nondeterministic_input(
    spark_fixture: SparkSession, nondeterministic_id
) -> None:
    # every evaluation produces new values, so none match the counting pass
    sdf = spark_fixture.range(50).select(nondeterministic_id().alias("partition_col"))

    result = group_and_batch_spark_dataframe(sdf, "partition_col", num_groups=4)

    rows = result.collect()
    assert all(row["group"] is not None and 0 <= row["group"] < 4 for row in rows)
    assert all(row["batch"] == 0 for row in rows)


def test_greedy_null_partition_value_yields_null_group(
    spark_fixture: SparkSession,
) -> None:
    sdf = spark_fixture.createDataFrame(
        [("a",), ("a",), ("b",), (None,)], "partition_col string"
    )

    result = create_node_groupings(sdf, "partition_col", num_groups=4)

    groups = {row["partition_col"]: row["group"] for row in result.collect()}
    assert groups[None] is None
    assert groups["a"] is not None and groups["b"] is not None
