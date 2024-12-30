from typing import Dict

from pyspark.sql import DataFrame, SparkSession


def create_value_groupings(
    value_counts_spark_dataframe: DataFrame, spark: SparkSession, num_groups: int
):
    # to create buckets
    # # track with 2 separate hash maps
    counts_bucket = {i: 0 for i in range(num_groups)}
    keys_bucket = {i: list() for i in range(num_groups)}
    # stack source and target
    # group by and count

    counts_df = value_counts_spark_dataframe.orderBy("count", ascending=False)
    # iterate through the values in max -> min order ex: [{key: Amazon, value_count: 100000}, ...]
    # find most-empty bucket (num_groups) and place value in it and increment bucket value by value_count
    for row in counts_df.collect():
        smallest_bucket_id = _get_smallest_bucket_id(counts_bucket)
        counts_bucket[smallest_bucket_id] = (
            counts_bucket.get(smallest_bucket_id) + row["count"]
        )
        keys_bucket.get(smallest_bucket_id).append(row["combined_col"])

    key_to_group_map = list()
    for bucket_id, lst in keys_bucket.items():
        for v in lst:
            key_to_group_map.append({"value": v, "group": bucket_id})

    return spark.createDataFrame(key_to_group_map)


def _get_smallest_bucket_id(bucket: Dict[int, int]) -> int:
    """Return the key of the smallest bucket."""

    min_val = float("inf")
    min_bucket_id = 0
    for k, v in bucket.items():
        if v < min_val:
            min_val = v
            min_bucket_id = k

    return min_bucket_id
