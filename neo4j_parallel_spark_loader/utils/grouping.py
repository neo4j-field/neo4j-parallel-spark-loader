import heapq
from dataclasses import dataclass
from functools import reduce
from operator import or_
from typing import Dict, Iterable, List, Optional, Tuple, Union

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    concat,
    count,
    explode,
    lit,
    pmod,
    struct,
    when,
    xxhash64,
)
from pyspark.sql.functions import sum as sum_
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


@dataclass(frozen=True)
class GroupingResult:
    """
    A grouped DataFrame plus what grouping learned about it, for building an `IngestPlan`.

    Attributes
    ----------
    dataframe : DataFrame
        The grouped DataFrame.
    group_counts : List[int]
        The number of groups each group column draws from: `[n]` for predefined components
        and monopartite, `[source n, target n]` for bipartite. Every non-null group value in a
        column is in `[0, n)`.
    total_rows : Optional[int]
        The number of rows, when grouping counted them (greedy strategy).
    null_rows : Optional[int]
        The number of rows with a `null` group, when grouping counted them (greedy strategy).
    """

    dataframe: DataFrame
    group_counts: List[int]
    total_rows: Optional[int] = None
    null_rows: Optional[int] = None

    def with_row_counts(self, columns: List[str]) -> "GroupingResult":
        """
        Return this result with `total_rows` and `null_rows` filled in, counting them in one
        pass over the grouped DataFrame if grouping did not. A row has a `null` group exactly
        when one of `columns` is `null`.
        """

        if self.total_rows is not None and self.null_rows is not None:
            return self
        total_rows, null_rows = count_rows_and_null_keys(self.dataframe, columns)
        return GroupingResult(self.dataframe, self.group_counts, total_rows, null_rows)


@dataclass(frozen=True)
class KeyGroupings:
    """
    The result of `create_key_groupings`.

    Attributes
    ----------
    mappings : List[Tuple[DataFrame, int]]
        One `(mapping DataFrame, group count)` pair per key set, in the order the key sets
        were given. Each mapping DataFrame maps a key to its `group`, and groups are numbered
        `0` to `group count - 1`.
    total_rows : int
        The number of rows in the DataFrame.
    null_rows : int
        The number of rows with a `null` key in any key column. These rows get a `null` group.
    """

    mappings: List[Tuple[DataFrame, int]]
    total_rows: int
    null_rows: int


def create_key_groupings(
    spark_dataframe: DataFrame, key_sets: List[List[Column]], num_groups: int
) -> KeyGroupings:
    """
    Greedily bin-pack grouping keys into `num_groups` balanced groups, in a single pass.

    Each key set is bin-packed independently. Within a key set, the keys from every column are
    pooled and counted together, so a key that appears in several columns (e.g. as both source
    and target of a monopartite relationship) receives a single group. Pass one key set per
    column when the columns refer to different nodes (e.g. bipartite source and target).

    Only `(key, count)` pairs of longs are collected to the driver. The same pass also counts
    the rows in the DataFrame and the rows with a `null` key, so ingest does not need a pass of
    its own to find them.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.
    key_sets : List[List[Column]]
        Key expressions, typically built with `value_key_column`, grouped into key sets.
    num_groups : int
        The desired number of groups per key set.

    Returns
    -------
    KeyGroupings
        The key to group mapping of each key set, and the total and `null` row counts.
    """

    key_columns = [key for key_set in key_sets for key in key_set]
    any_null_key = reduce(or_, [key.isNull() for key in key_columns])

    # one entry per (key set, key column) for every row; `first` marks one entry per row so
    # rows are counted once however many key columns there are
    entries = array(
        *[
            struct(
                lit(set_index).alias("key_set"),
                key.alias(_KEY_COLUMN),
                lit(set_index == 0 and column_index == 0).alias("first"),
            )
            for set_index, key_set in enumerate(key_sets)
            for column_index, key in enumerate(key_set)
        ]
    )
    first = col("first")
    key_counts = (
        spark_dataframe.select(explode(entries).alias("entry"), any_null_key.alias("n"))
        .select("entry.*", "n")
        .groupBy("key_set", _KEY_COLUMN)
        .agg(
            count(lit(1)).alias("count"),
            sum_(when(first, 1).otherwise(0)).alias("rows"),
            sum_(when(first & col("n"), 1).otherwise(0)).alias("null_rows"),
        )
        .collect()
    )

    schema = StructType(
        [
            StructField(_KEY_COLUMN, LongType(), nullable=False),
            StructField("group", LongType(), nullable=False),
        ]
    )
    mappings = []
    for set_index in range(len(key_sets)):
        key_to_group = _greedy_bin_pack(
            (
                (row[_KEY_COLUMN], row["count"])
                for row in key_counts
                if row["key_set"] == set_index and row[_KEY_COLUMN] is not None
            ),
            num_groups,
        )
        mapping_sdf = spark_dataframe.sparkSession.createDataFrame(
            list(key_to_group.items()), schema
        )
        mappings.append((mapping_sdf, len(set(key_to_group.values()))))

    return KeyGroupings(
        mappings=mappings,
        total_rows=sum(row["rows"] for row in key_counts),
        null_rows=sum(row["null_rows"] for row in key_counts),
    )


def count_rows_and_null_keys(
    spark_dataframe: DataFrame, columns: List[str]
) -> Tuple[int, int]:
    """
    Count the rows in the DataFrame and the rows with a `null` value in any of `columns`, in a
    single pass. Used by the hash strategy, which has no counting pass of its own.
    """

    any_null = reduce(or_, [col(column).isNull() for column in columns])
    row = spark_dataframe.agg(
        count(lit(1)).alias("rows"),
        sum_(when(any_null, 1).otherwise(0)).alias("null_rows"),
    ).collect()[0]

    return row["rows"], row["null_rows"] or 0


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
