from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from pyspark.sql.functions import col, countDistinct, spark_partition_id
from pytest_mock import MockerFixture

from neo4j_parallel_spark_loader.bipartite import (
    group_and_batch_spark_dataframe as group_and_batch_bipartite,
)
from neo4j_parallel_spark_loader.monopartite import (
    group_and_batch_spark_dataframe as group_and_batch_monopartite,
)
from neo4j_parallel_spark_loader.predefined_components import (
    group_and_batch_spark_dataframe as group_and_batch_predefined,
)
from neo4j_parallel_spark_loader.utils.ingest import (
    _partition_keys,
    ingest_spark_dataframe,
)


def test_invalid_mode(utils_complete_spark_dataframe: DataFrame) -> None:
    with pytest.raises(AssertionError) as e:
        ingest_spark_dataframe(
            spark_dataframe=utils_complete_spark_dataframe,
            save_mode="Wrong",
            options={},
        )

    assert "`save_mode` must be either 'Append' or 'Overwrite'" in str(e)


def test_invalid_columns(
    utils__spark_dataframe_missing_batch_column: DataFrame,
) -> None:
    with pytest.raises(AssertionError) as e:
        ingest_spark_dataframe(
            spark_dataframe=utils__spark_dataframe_missing_batch_column,
            save_mode="Append",
            options={},
        )

    assert "Spark DataFrame must contain column `batch`" in str(e)


def test_null_batch_raises_by_default(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [(1, 6, "0 --> 1", 0), (None, 7, None, None), (2, None, None, None)],
        "source_node int, target_node int, group string, batch int",
    )

    with pytest.raises(ValueError) as e:
        ingest_spark_dataframe(
            spark_dataframe=sdf,
            save_mode="Append",
            options={},
            num_groups=1,
        )

    assert "2 row(s) have a null `batch` or `group`" in str(e.value)


def test_null_batch_skip_warns_and_writes_remaining_batches(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    sdf = spark_fixture.createDataFrame(
        [
            (1, 6, "0 --> 1", 0),
            (1, 7, "0 --> 2", 1),
            (None, 7, None, None),
        ],
        "source_node int, target_node int, group string, batch int",
    )
    mock_write = mocker.patch.object(
        DataFrame, "write", new_callable=mocker.PropertyMock
    )

    with pytest.warns(UserWarning, match="1 row\\(s\\) have a null `batch` or `group`"):
        ingest_spark_dataframe(
            spark_dataframe=sdf,
            save_mode="Append",
            options={},
            num_groups=1,
            on_null_batch="skip",
        )

    # one save per non-null batch value
    assert (
        mock_write.return_value.mode.return_value.format.return_value.options.return_value.save.call_count
        == 2
    )


def test_invalid_on_null_batch(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [(1, 6, "0 --> 1", 0)],
        "source_node int, target_node int, group string, batch int",
    )
    with pytest.raises(AssertionError) as e:
        ingest_spark_dataframe(
            spark_dataframe=sdf,
            save_mode="Append",
            options={},
            on_null_batch="ignore",
        )

    assert "`on_null_batch` must be either 'raise' or 'skip'" in str(e)


def _capture_neo4j_saves(mocker: MockerFixture) -> List[DataFrame]:
    """
    Replace `DataFrameWriter.save` (the Neo4j connector write) with a recorder of the
    DataFrame being written. Parquet writes use `DataFrameWriter.parquet` and are unaffected.
    """
    captured: List[DataFrame] = []

    def fake_save(self, *args, **kwargs):
        captured.append(self._df)
        return MagicMock()

    mocker.patch.object(DataFrameWriter, "save", fake_save)
    return captured


def _assert_one_group_per_partition(batch_df: DataFrame) -> None:
    group_count = batch_df.select("group").distinct().count()
    per_partition = (
        batch_df.withColumn("pid", spark_partition_id())
        .groupBy("pid")
        .agg(countDistinct("group").alias("groups"))
        .collect()
    )
    assert batch_df.rdd.getNumPartitions() == group_count
    assert len(per_partition) == group_count, "some partitions are empty"
    assert all(
        row["groups"] == 1 for row in per_partition
    ), "a partition holds >1 group"


@pytest.mark.parametrize("partition_count", [1, 2, 7, 10, 16, 33])
def test_partition_keys_land_in_distinct_partitions(
    spark_fixture: SparkSession, partition_count: int
) -> None:
    keys = _partition_keys(spark_fixture, partition_count)
    assert len(keys) == partition_count
    assert len(set(keys)) == partition_count

    key_df = spark_fixture.createDataFrame([(k,) for k in keys], "key long")
    occupied = (
        key_df.repartition(partition_count, col("key"))
        .select(spark_partition_id().alias("pid"))
        .distinct()
        .count()
    )
    assert occupied == partition_count


@pytest.mark.parametrize(
    "group_and_batch, num_groups",
    [(group_and_batch_bipartite, 8), (group_and_batch_monopartite, 10)],
)
def test_ingest_writes_each_group_from_its_own_partition(
    spark_fixture: SparkSession,
    mocker: MockerFixture,
    group_and_batch,
    num_groups: int,
) -> None:
    sdf = spark_fixture.range(3000).selectExpr(
        "cast(id * 7919 % 211 as string) as src",
        "cast(id * 104729 % 389 as string) as tgt",
    )
    grouped = group_and_batch(sdf, "src", "tgt", num_groups, strategy="hash")
    expected_batches = grouped.select("batch").distinct().count()

    captured = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(grouped, "Append", options={})

    assert len(captured) == expected_batches
    for batch_df in captured:
        assert batch_df.select("batch").distinct().count() == 1
        assert "__neo4j_parallel_group_key" not in batch_df.columns
        _assert_one_group_per_partition(batch_df)


def test_ingest_predefined_components_one_partition_per_group(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    sdf = spark_fixture.range(500).selectExpr(
        "id as src", "id + 1 as tgt", "cast(id % 37 as string) as component"
    )
    grouped = group_and_batch_predefined(sdf, "component", 6, strategy="hash")

    captured = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(grouped, "Append", options={})

    assert len(captured) == 1
    _assert_one_group_per_partition(captured[0])


def test_ingest_with_checkpoint(
    spark_fixture: SparkSession, mocker: MockerFixture, tmp_path: Path
) -> None:
    sdf = spark_fixture.range(1000).selectExpr(
        "cast(id % 97 as string) as src", "cast(id % 89 as string) as tgt"
    )
    grouped = group_and_batch_bipartite(sdf, "src", "tgt", 4, strategy="hash")
    checkpoint = tmp_path / "checkpoint"

    captured = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(
        grouped, "Append", options={}, checkpoint_path=str(checkpoint)
    )

    batch_dirs = sorted(
        p.name for p in checkpoint.iterdir() if p.name.startswith("batch=")
    )
    assert batch_dirs == ["batch=0", "batch=1", "batch=2", "batch=3"]
    assert len(captured) == 4
    assert sum(df.count() for df in captured) == 1000
    for batch_df in captured:
        _assert_one_group_per_partition(batch_df)


def test_predefined_null_partition_value_is_reported(
    spark_fixture: SparkSession,
) -> None:
    sdf = spark_fixture.createDataFrame(
        [(1, 2, "a"), (3, 4, None), (5, 6, "b")], "src int, tgt int, component string"
    )
    grouped = group_and_batch_predefined(sdf, "component", 4, strategy="hash")

    with pytest.raises(ValueError, match="1 row\\(s\\) have a null `batch` or `group`"):
        ingest_spark_dataframe(grouped, "Append", options={})


def test_ingest_sorts_rows_within_each_partition(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    sdf = spark_fixture.range(3000).selectExpr(
        "cast(id * 7919 % 211 as string) as src",
        "cast(id * 104729 % 389 as string) as tgt",
    )
    grouped = group_and_batch_bipartite(sdf, "src", "tgt", 4, strategy="hash")

    captured = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(grouped, "Append", options={}, sort_columns=["src"])

    assert captured
    for batch_df in captured:
        _assert_one_group_per_partition(batch_df)
        for partition in batch_df.select("src").rdd.glom().collect():
            values = [row["src"] for row in partition]
            assert values == sorted(values)


def test_ingest_rejects_unknown_sort_column(spark_fixture: SparkSession) -> None:
    sdf = spark_fixture.createDataFrame(
        [(1, 6, "0 --> 1", 0)],
        "source_node int, target_node int, group string, batch int",
    )
    with pytest.raises(AssertionError, match="sort column `nope` is not in"):
        ingest_spark_dataframe(sdf, "Append", options={}, sort_columns=["nope"])


def test_ingest_checkpoint_and_sort_columns_together(
    spark_fixture: SparkSession, mocker: MockerFixture, tmp_path: Path
) -> None:
    """`checkpoint_path` and `sort_columns` are independent features; verify the sort still
    holds once rows have made the round trip through the checkpoint's Parquet read-back."""
    sdf = spark_fixture.range(3000).selectExpr(
        "cast(id * 7919 % 211 as string) as src",
        "cast(id * 104729 % 389 as string) as tgt",
    )
    grouped = group_and_batch_bipartite(sdf, "src", "tgt", 4, strategy="hash")
    checkpoint = tmp_path / "checkpoint"

    captured = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(
        grouped,
        "Append",
        options={},
        checkpoint_path=str(checkpoint),
        sort_columns=["src"],
    )

    assert captured
    for batch_df in captured:
        _assert_one_group_per_partition(batch_df)
        for partition in batch_df.select("src").rdd.glom().collect():
            values = [row["src"] for row in partition]
            assert values == sorted(values)


def test_ingest_checkpoint_resume_after_partial_failure(
    spark_fixture: SparkSession, mocker: MockerFixture, tmp_path: Path
) -> None:
    """
    Exercises the resume workflow documented on `checkpoint_path`: read the checkpoint back,
    filter to the batches that were not yet written, and pass the result to
    `ingest_spark_dataframe` again.
    """
    sdf = spark_fixture.range(1000).selectExpr(
        "cast(id % 97 as string) as src", "cast(id % 89 as string) as tgt"
    )
    grouped = group_and_batch_bipartite(sdf, "src", "tgt", 4, strategy="hash")
    checkpoint = tmp_path / "checkpoint"

    # first attempt writes the checkpoint; pretend only batches 0 and 1 made it to Neo4j
    # before the job died (e.g. a driver crash after some batches finished writing)
    captured_first = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(
        grouped, "Append", options={}, checkpoint_path=str(checkpoint)
    )
    assert len(captured_first) == 4
    succeeded_batches = [0, 1]

    # resume: read the checkpoint back and filter to the batches that never completed
    checkpoint_df = spark_fixture.read.parquet(str(checkpoint))
    remaining = checkpoint_df.filter(~col("batch").isin(succeeded_batches))
    expected_remaining_count = remaining.count()
    assert 0 < expected_remaining_count < grouped.count()

    captured_resume = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(remaining, "Append", options={})

    resumed_batches = {
        row["batch"]
        for df in captured_resume
        for row in df.select("batch").distinct().collect()
    }
    assert resumed_batches == {2, 3}
    assert sum(df.count() for df in captured_resume) == expected_remaining_count


def test_ingest_empty_dataframe_is_a_no_op(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    sdf = spark_fixture.createDataFrame([], "src string, tgt string")
    grouped = group_and_batch_bipartite(sdf, "src", "tgt", 4, strategy="hash")

    captured = _capture_neo4j_saves(mocker)
    ingest_spark_dataframe(grouped, "Append", options={})

    assert captured == []
