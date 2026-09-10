from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from neo4j_parallel_spark_loader.utils.hash_grouping import hash_group_column


def test_hash_group_column_range(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [{"node_id": i} for i in range(500)],
    )
    num_groups = 7

    result: DataFrame = sdf.withColumn(
        "group", hash_group_column("node_id", num_groups)
    )

    assert result.filter(col("group").isNull()).count() == 0
    assert result.filter((col("group") < 0) | (col("group") >= num_groups)).count() == 0


def test_hash_group_column_deterministic(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [{"node_id": i} for i in range(200)],
    )
    num_groups = 5

    first_result = (
        sdf.withColumn("group", hash_group_column("node_id", num_groups))
        .orderBy("node_id")
        .collect()
    )
    second_result = (
        sdf.withColumn("group", hash_group_column("node_id", num_groups))
        .orderBy("node_id")
        .collect()
    )

    assert first_result == second_result


def test_hash_group_column_null_stays_null(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [{"node_id": 1}, {"node_id": None}],
        "node_id: bigint",
    )

    result = sdf.withColumn("group", hash_group_column("node_id", 4))

    null_row = result.filter(col("node_id").isNull()).collect()[0]
    assert null_row["group"] is None
