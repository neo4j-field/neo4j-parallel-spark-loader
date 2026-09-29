import heapq
from typing import Dict, Iterable, List, Tuple, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, concat, lit, pmod, when, xxhash64
from pyspark.sql.types import LongType, StructField, StructType

_KEY_COLUMN = "__neo4j_parallel_key"
_MAPPED_GROUP_COLUMN = "__neo4j_parallel_mapped_group"


def value_key_column(column: Union[str, Column]) -> Column:
    """
    Compute the grouping key for a node id: `xxhash64` of the value, or `null` for a `null` value.

    The greedy strategy used to collect raw id values to the driver and join the resulting
    groups back onto the data by value. A value that did not survive that round trip unchanged
    (for example a string containing invalid UTF-8, which Python decodes with replacement
    characters) matched nothing and was left with a `null` group. Grouping on a long computed
    inside Spark means the ids never leave the JVM.

    Two values sharing a hash are simply counted and grouped together. Putting extra values in
    the same group never breaks the loading invariant (all rows touching a node share a
    group), it can only affect balance slightly.

    `xxhash64(null)` returns the seed rather than `null`, so `null` values are mapped to a `null`
    key explicitly and still end up with a `null` group.
    """

    value = col(column) if isinstance(column, str) else column
    return when(value.isNull(), lit(None).cast(LongType())).otherwise(xxhash64(value))


def create_key_groupings(
    spark_dataframe: DataFrame, key_columns: List[Column], num_groups: int
) -> Tuple[DataFrame, int]:
    """
    Greedily bin-pack grouping keys into `num_groups` balanced groups.

    The keys from every column in `key_columns` are pooled and counted together, so a key that
    appears in several columns (e.g. as both source and target of a monopartite relationship)
    receives a single group. Only `(key, count)` pairs of longs are collected to the driver.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    key_columns : List[Column]
        Key expressions, typically built with `value_key_column`.
    num_groups : int
        The desired number of groups.

    Returns
    -------
    Tuple[DataFrame, int]
        A DataFrame mapping each key to its `group`, and the number of groups actually used.
        Groups are numbered `0` to `group_count - 1`.
    """

    keys_sdf = None
    for key_column in key_columns:
        column_keys = spark_dataframe.select(key_column.alias(_KEY_COLUMN))
        keys_sdf = column_keys if keys_sdf is None else keys_sdf.unionAll(column_keys)

    key_counts = (
        keys_sdf.where(col(_KEY_COLUMN).isNotNull())
        .groupBy(_KEY_COLUMN)
        .count()
        .collect()
    )
    key_to_group = _greedy_bin_pack(
        ((row[_KEY_COLUMN], row["count"]) for row in key_counts), num_groups
    )
    group_count = len(set(key_to_group.values()))

    schema = StructType(
        [
            StructField(_KEY_COLUMN, LongType(), nullable=False),
            StructField("group", LongType(), nullable=False),
        ]
    )
    mapping_sdf = spark_dataframe.sparkSession.createDataFrame(
        list(key_to_group.items()), schema
    )

    return mapping_sdf, group_count


def apply_key_groupings(
    spark_dataframe: DataFrame,
    key_column: Column,
    mapping_sdf: DataFrame,
    group_count: int,
    output_column: str,
) -> DataFrame:
    """
    Add `output_column` holding the group of each row's key according to `mapping_sdf`.

    A `null` key gets a `null` group. A non-null key missing from the mapping gets the fallback
    group `pmod(key, group_count)`. That happens when the input is non-deterministic and a
    re-evaluation produces values that were not present when the groups were counted. Because
    the mapping is a fixed local relation, a key's group depends only on the key, so all rows
    sharing a value still land in the same group. The fallback stays within the groups the
    mapping uses, which the batching schemes rely on.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    key_column : Column
        The key expression, built the same way as the keys passed to `create_key_groupings`.
    mapping_sdf : DataFrame
        The key to group mapping returned by `create_key_groupings`.
    group_count : int
        The group count returned by `create_key_groupings`.
    output_column : str
        The name of the group column to add.

    Returns
    -------
    DataFrame
        The Spark DataFrame with `output_column` added.
    """

    fallback_group = pmod(col(_KEY_COLUMN), lit(max(group_count, 1))).cast(LongType())

    return (
        spark_dataframe.withColumn(_KEY_COLUMN, key_column)
        .join(
            mapping_sdf.withColumnRenamed("group", _MAPPED_GROUP_COLUMN),
            on=_KEY_COLUMN,
            how="left",
        )
        .withColumn(
            output_column,
            when(col(_KEY_COLUMN).isNull(), lit(None).cast(LongType())).otherwise(
                coalesce(col(_MAPPED_GROUP_COLUMN), fallback_group)
            ),
        )
        .drop(_KEY_COLUMN, _MAPPED_GROUP_COLUMN)
    )


def _greedy_bin_pack(
    key_counts: Iterable[Tuple[int, int]], num_groups: int
) -> Dict[int, int]:
    """
    Assign each key to a group, largest count first, always into the group with the fewest
    rows so far (the lowest group id on ties). Keys are ordered by descending count, then by
    key, so the assignment is reproducible.
    """

    buckets = [(0, group) for group in range(num_groups)]
    key_to_group = {}
    for key, key_count in sorted(key_counts, key=lambda kc: (-kc[1], kc[0])):
        bucket_count, group = heapq.heappop(buckets)
        key_to_group[key] = group
        heapq.heappush(buckets, (bucket_count + key_count, group))

    return key_to_group


def create_group_column_from_source_and_target_groups(
    spark_dataframe: DataFrame,
) -> DataFrame:
    """Add a `group` column to the Spark DataFrame."""

    return spark_dataframe.withColumn(
        "group", concat(col("source_group"), lit(" --> "), col("target_group"))
    )


def create_value_counts_dataframe(
    spark_dataframe: DataFrame, grouping_column: str
) -> DataFrame:
    """
    Create a `count` column based on the `grouping_column` argument.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    grouping_column : str
        The grouping column to use.

    Returns
    -------
    DataFrame
        The value counts Spark DataFrame.
    """
    sdf_filtered = spark_dataframe.select(grouping_column)

    counts_df: DataFrame = sdf_filtered.groupBy(grouping_column).count()

    return counts_df
