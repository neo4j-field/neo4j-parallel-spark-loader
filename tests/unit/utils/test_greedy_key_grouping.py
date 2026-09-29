from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, when
from pyspark.sql.types import LongType

from neo4j_parallel_spark_loader.utils.grouping import (
    _greedy_bin_pack,
    apply_key_groupings,
    create_key_groupings,
    value_key_column,
)

# `unhex('C328')` is the byte 0xC3 followed by "(", which is not valid UTF-8. Python decodes it
# with a replacement character, so the value does not survive a round trip through the driver.
INVALID_UTF8 = "cast(unhex('C328') AS STRING)"


def test_greedy_bin_pack_balances_groups() -> None:
    key_counts = [(1, 5), (2, 4), (3, 3), (4, 3), (5, 2), (6, 1)]

    key_to_group = _greedy_bin_pack(key_counts, 3)

    loads = {}
    for key, key_count in key_counts:
        group = key_to_group[key]
        loads[group] = loads.get(group, 0) + key_count
    assert sorted(loads.values()) == [6, 6, 6]


def test_greedy_bin_pack_is_order_independent() -> None:
    key_counts = [(10, 2), (11, 2), (12, 2), (13, 1)]

    assert _greedy_bin_pack(key_counts, 2) == _greedy_bin_pack(key_counts[::-1], 2)


def test_greedy_bin_pack_uses_lowest_groups_when_fewer_keys_than_groups() -> None:
    key_to_group = _greedy_bin_pack([(7, 1), (8, 3)], 5)

    assert sorted(key_to_group.values()) == [0, 1]


def test_value_key_column_null_stays_null(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame([("a",), (None,)], "value string")

    rows = sdf.select("value", value_key_column("value").alias("key")).collect()

    keys = {row["value"]: row["key"] for row in rows}
    assert keys["a"] is not None
    assert keys[None] is None


def test_create_key_groupings_counts_keys_from_all_columns(
    spark_fixture: SparkSession,
) -> None:
    sdf = spark_fixture.createDataFrame([(1, 2), (1, 3), (2, 1)], "s long, t long")

    mapping_sdf, group_count = create_key_groupings(
        spark_dataframe=sdf,
        key_columns=[value_key_column("s"), value_key_column("t")],
        num_groups=4,
    )

    assert mapping_sdf.count() == 3
    assert group_count == 3


def test_invalid_utf8_values_get_a_group(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.range(20).select(
        expr(
            f"CASE WHEN id % 4 = 0 THEN {INVALID_UTF8} ELSE cast(id % 3 AS STRING) END"
        ).alias("value")
    )
    key = value_key_column("value")
    mapping_sdf, group_count = create_key_groupings(
        spark_dataframe=sdf, key_columns=[key], num_groups=4
    )

    result = apply_key_groupings(
        spark_dataframe=sdf,
        key_column=key,
        mapping_sdf=mapping_sdf,
        group_count=group_count,
        output_column="group",
    )

    assert result.filter(col("group").isNull()).count() == 0
    assert result.select("value", "group").distinct().count() == 4


def test_colliding_keys_share_a_group(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [("a",), ("b",), ("c",), (None,)], "value string"
    )
    # every non-null value collides on the same key
    key = when(col("value").isNull(), lit(None).cast(LongType())).otherwise(
        lit(7).cast(LongType())
    )
    mapping_sdf, group_count = create_key_groupings(
        spark_dataframe=sdf, key_columns=[key], num_groups=4
    )

    result = apply_key_groupings(
        spark_dataframe=sdf,
        key_column=key,
        mapping_sdf=mapping_sdf,
        group_count=group_count,
        output_column="group",
    )

    groups = {row["value"]: row["group"] for row in result.collect()}
    assert group_count == 1
    assert groups["a"] == groups["b"] == groups["c"] == 0
    assert groups[None] is None


def test_unmatched_keys_fall_back_within_group_count(
    spark_fixture: SparkSession,
) -> None:
    counted_sdf = spark_fixture.createDataFrame(
        [("a",), ("a",), ("b",), ("c",)], "value string"
    )
    key = value_key_column("value")
    mapping_sdf, group_count = create_key_groupings(
        spark_dataframe=counted_sdf, key_columns=[key], num_groups=10
    )
    # values the counting pass never saw, as a non-deterministic input would produce
    unseen_sdf = spark_fixture.range(200).select(
        expr("concat('unseen-', cast(id % 50 AS STRING))").alias("value")
    )

    result = apply_key_groupings(
        spark_dataframe=unseen_sdf,
        key_column=key,
        mapping_sdf=mapping_sdf,
        group_count=group_count,
        output_column="group",
    )

    assert group_count == 3
    assert result.filter(col("group").isNull()).count() == 0
    assert result.filter((col("group") < 0) | (col("group") >= 3)).count() == 0
    # each value still lands in exactly one group
    assert result.select("value", "group").distinct().count() == 50
