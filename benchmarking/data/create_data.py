from functools import reduce
from random import randint

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_set, concat, expr, lit, rand
from pyspark.sql.types import IntegerType, StructField, StructType


def power_law_numbers(n: int, min_value: int, max_value: int, alpha: float):
    # Generate power law distributed numbers between 0 and 1
    # alpha is the power law exponent (typically between 2 and 3)
    raw_numbers = np.random.power(alpha, n)

    # Scale to desired range
    scaled_numbers = (raw_numbers * (max_value - min_value) + min_value).astype(int)
    return scaled_numbers


def generate_unpartitioned_dataframe(spark: SparkSession, num_rows: int) -> DataFrame:
    n_relationships = num_rows * 100

    df = spark.range(0, n_relationships).select(
        expr("id"), rand(seed=42).alias("r1"), rand(seed=43).alias("r2")
    )

    # Convert uniform random numbers to power law using inverse transform
    # x = x_min * (1-r)^(-1/(alpha-1))
    # For alpha=2.0, this simplifies to x = x_min / r
    df = df.select(
        expr("cast(10.0 / r1 as int) as source"),
        expr("cast(10.0 / r2 as int) as target"),
    ).select("source", "target")

    # Cache the result
    rel_df = df.dropDuplicates().limit(num_rows).cache()

    return rel_df


def generate_partitioned_dataframe(
    spark: SparkSession, num_rows: int, partition_count: int
) -> DataFrame:
    mean_partition_size = num_rows / partition_count
    min_partition_size = int(mean_partition_size * 0.8)
    max_partition_size = int(mean_partition_size * 1.2)
    partition_sizes = [
        randint(min_partition_size, max_partition_size)
        for _ in range(0, partition_count - 1)
    ]
    partition_sizes.append(num_rows - sum(partition_sizes))
    partition_dfs = []
    for i, partition_size in enumerate(partition_sizes):
        partition_df = generate_unpartitioned_dataframe(
            spark=spark, num_rows=partition_size
        )
        partition_df = partition_df.withColumn("partition_col", lit(i))
        partition_df = partition_df.withColumn(
            "source", concat(lit(str(i) + "-"), partition_df["source"])
        )
        partition_df = partition_df.withColumn(
            "target", concat(lit(str(i) + "-"), partition_df["target"])
        )
        partition_dfs.append(partition_df)

    return reduce(DataFrame.unionAll, partition_dfs)


if __name__ == "__main__":
    NUM_ROWS = 1_000_000
    PARTITION_COUNT = 20
    root = "benchmarking/data/"
    spark: SparkSession = SparkSession.builder.appName("benchmarking").getOrCreate()

    bipartite_df = generate_unpartitioned_dataframe(spark=spark, num_rows=NUM_ROWS)
    bipartite_df.toPandas().to_csv(
        root + "bipartite_data.csv", header=True, index=False
    )

    monopartite_df = generate_unpartitioned_dataframe(spark=spark, num_rows=NUM_ROWS)
    monopartite_df.toPandas().to_csv(
        root + "monopartite_data.csv", header=True, index=False
    )

    predefined_components_df = generate_partitioned_dataframe(
        spark=spark, num_rows=NUM_ROWS, partition_count=PARTITION_COUNT
    )
    predefined_components_df.toPandas().to_csv(
        root + "predefined_components_data.csv", header=True, index=False
    )
