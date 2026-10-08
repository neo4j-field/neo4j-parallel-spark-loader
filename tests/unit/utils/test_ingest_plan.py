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
from neo4j_parallel_spark_loader.bipartite.batching import (
    plan_ingest_batches as plan_bipartite_batches,
)
from neo4j_parallel_spark_loader.monopartite import (
    group_and_batch_spark_dataframe as group_and_batch_monopartite,
)
from neo4j_parallel_spark_loader.monopartite.batching import (
    create_ingest_batches_from_groups as batch_monopartite_groups,
)
from neo4j_parallel_spark_loader.monopartite.batching import (
    plan_ingest_batches as plan_monopartite_batches,
)
from neo4j_parallel_spark_loader.monopartite.grouping import _create_group_column
from neo4j_parallel_spark_loader.predefined_components import (
    group_and_batch_spark_dataframe as group_and_batch_predefined,
)
from neo4j_parallel_spark_loader.utils.build_relationship import build_relationship
from neo4j_parallel_spark_loader.utils.ingest import ingest_spark_dataframe
from neo4j_parallel_spark_loader.utils.ingest_plan import IngestPlan


def _edges(spark: SparkSession, rows: int = 2000) -> DataFrame:
    """Edges with string ids, a component column, and a sprinkling of null ids."""
    return spark.range(rows).selectExpr(
        "CASE WHEN id % 97 = 0 THEN NULL ELSE cast(id * 7919 % 211 AS STRING) END AS src",
        "CASE WHEN id % 89 = 0 THEN NULL ELSE cast(id * 104729 % 389 AS STRING) END "
        "AS tgt",
        "CASE WHEN id % 83 = 0 THEN NULL ELSE cast(id % 41 AS STRING) END AS component",
    )


def _group(scenario: str, sdf: DataFrame, num_groups: int, strategy: str):
    if scenario == "predefined":
        return group_and_batch_predefined(
            sdf, "component", num_groups, strategy=strategy, return_plan=True
        )
    group_and_batch = (
        group_and_batch_bipartite
        if scenario == "bipartite"
        else group_and_batch_monopartite
    )
    return group_and_batch(
        sdf, "src", "tgt", num_groups, strategy=strategy, return_plan=True
    )


def _capture_neo4j_saves(mocker: MockerFixture) -> List[DataFrame]:
    """
    Record each batch's DataFrame as it is about to be written to Neo4j, with its `batch`
    and `group` columns still present so tests can inspect scheduling and partitioning.
    The write itself is `DataFrameWriter.save` (the Neo4j connector); `batch` and `group`
    are dropped just before it, so that is the last point they can be observed. Use
    `_capture_written_frames` to see exactly what reaches the connector. Parquet writes use
    `DataFrameWriter.parquet` and are unaffected.
    """
    captured: List[DataFrame] = []
    original_drop = DataFrame.drop

    def spy_drop(self, *cols):
        if cols == ("batch", "group"):
            captured.append(self)
        return original_drop(self, *cols)

    def fake_save(self, *args, **kwargs):
        return MagicMock()

    mocker.patch.object(DataFrame, "drop", spy_drop)
    mocker.patch.object(DataFrameWriter, "save", fake_save)
    return captured


def _capture_written_frames(mocker: MockerFixture) -> List[DataFrame]:
    """Record the DataFrame that actually reaches the Neo4j connector for each batch."""
    written: List[DataFrame] = []

    def fake_save(self, *args, **kwargs):
        written.append(self._df)
        return MagicMock()

    mocker.patch.object(DataFrameWriter, "save", fake_save)
    return written


def _batch_contents(captured: List[DataFrame]) -> dict:
    """Map each written batch to the sorted rows written in it."""
    contents = {}
    for batch_df in captured:
        rows = sorted((tuple(row) for row in batch_df.collect()), key=repr)
        [batch] = {row["batch"] for row in batch_df.select("batch").collect()}
        contents[batch] = rows
    return contents


@pytest.mark.parametrize("strategy", ["greedy", "hash"])
@pytest.mark.parametrize(
    "scenario, num_groups",
    [
        ("predefined", 5),
        ("bipartite", 4),
        ("bipartite", 5),
        ("monopartite", 4),
        ("monopartite", 5),
    ],
)
def test_plan_describes_grouped_dataframe(
    spark_fixture: SparkSession, scenario: str, num_groups: int, strategy: str
) -> None:
    sdf = _edges(spark_fixture)

    grouped, plan = _group(scenario, sdf, num_groups, strategy)

    observed = {
        (row["batch"], row["group"])
        for row in grouped.where(col("batch").isNotNull() & col("group").isNotNull())
        .select("batch", "group")
        .distinct()
        .collect()
    }
    planned = {
        (batch, group) for batch, groups in plan.batches.items() for group in groups
    }
    assert observed <= planned
    # dense data fills every planned batch
    assert {batch for batch, _ in observed} == set(plan.batches)
    assert plan.total_rows == sdf.count()
    assert (
        plan.null_rows
        == grouped.where(col("batch").isNull() | col("group").isNull()).count()
    )
    assert plan.null_rows > 0


@pytest.mark.parametrize("group_count", [1, 2, 3, 4, 5, 6, 7, 8])
def test_monopartite_plan_matches_every_group_pair(
    spark_fixture: SparkSession, group_count: int
) -> None:
    # every (source group, target group) pair, batched directly so that each group the
    # batching can produce is present
    pairs = [(s, t) for s in range(group_count) for t in range(group_count)]
    sdf = spark_fixture.createDataFrame(pairs, "source_group long, target_group long")
    batched = batch_monopartite_groups(
        _create_group_column(sdf), known_group_count=group_count
    )

    observed = {}
    for row in batched.select("batch", "group").distinct().collect():
        observed.setdefault(row["batch"], set()).add(row["group"])

    planned = plan_monopartite_batches(group_count)
    assert {batch: set(groups) for batch, groups in planned.items()} == observed


def test_bipartite_plan_with_unequal_group_counts() -> None:
    batches = plan_bipartite_batches(2, 3)

    assert batches == {
        0: ["0 --> 0", "1 --> 2"],
        1: ["0 --> 1", "1 --> 0"],
        2: ["0 --> 2", "1 --> 1"],
    }


def test_plans_for_empty_dataframe(spark_fixture: SparkSession) -> None:
    sdf = _edges(spark_fixture).limit(0)

    for scenario in ["predefined", "bipartite", "monopartite"]:
        _, plan = _group(scenario, sdf, 4, "greedy")
        assert plan == IngestPlan(batches={}, total_rows=0, null_rows=0)


def test_return_plan_false_returns_only_the_dataframe(
    spark_fixture: SparkSession,
) -> None:
    grouped = group_and_batch_bipartite(_edges(spark_fixture), "src", "tgt", 3)

    assert isinstance(grouped, DataFrame)


@pytest.mark.parametrize("scenario", ["predefined", "bipartite", "monopartite"])
def test_ingest_with_plan_writes_the_same_batches_as_without(
    spark_fixture: SparkSession, mocker: MockerFixture, scenario: str
) -> None:
    sdf = _edges(spark_fixture)
    grouped, plan = _group(scenario, sdf, 4, "greedy")

    captured_without = _capture_neo4j_saves(mocker)
    with pytest.warns(UserWarning, match=f"{plan.null_rows} row\\(s\\)"):
        ingest_spark_dataframe(grouped, "Append", options={}, on_null_batch="skip")
    captured_with = _capture_neo4j_saves(mocker)
    with pytest.warns(UserWarning, match=f"{plan.null_rows} row\\(s\\)"):
        ingest_spark_dataframe(
            grouped, "Append", options={}, on_null_batch="skip", plan=plan
        )

    assert _batch_contents(captured_with) == _batch_contents(captured_without)
    for batch_df in captured_with:
        per_partition = (
            batch_df.withColumn("pid", spark_partition_id())
            .groupBy("pid")
            .agg(countDistinct("group").alias("groups"))
            .collect()
        )
        assert all(row["groups"] == 1 for row in per_partition)


def test_ingest_with_plan_skips_the_schedule_pass(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    grouped, plan = _group("bipartite", _edges(spark_fixture), 3, "greedy")
    _capture_neo4j_saves(mocker)
    group_by = mocker.spy(DataFrame, "groupBy")

    with pytest.warns(UserWarning):
        ingest_spark_dataframe(
            grouped, "Append", options={}, on_null_batch="skip", plan=plan
        )

    assert not any(
        call.args[1:] == ("batch", "group") for call in group_by.call_args_list
    )


def test_ingest_plan_null_rows_raise_before_writing_anything(
    spark_fixture: SparkSession, mocker: MockerFixture, tmp_path: Path
) -> None:
    grouped, plan = _group("predefined", _edges(spark_fixture), 4, "greedy")
    captured = _capture_neo4j_saves(mocker)
    checkpoint = tmp_path / "checkpoint"

    with pytest.raises(
        ValueError, match=f"{plan.null_rows} row\\(s\\) have a null `batch` or `group`"
    ):
        ingest_spark_dataframe(
            grouped, "Append", options={}, plan=plan, checkpoint_path=str(checkpoint)
        )

    assert captured == []
    assert not checkpoint.exists()


def test_ingest_with_plan_and_checkpoint(
    spark_fixture: SparkSession, mocker: MockerFixture, tmp_path: Path
) -> None:
    sdf = _edges(spark_fixture).where(col("src").isNotNull() & col("tgt").isNotNull())
    grouped, plan = _group("bipartite", sdf, 3, "hash")
    captured = _capture_neo4j_saves(mocker)

    ingest_spark_dataframe(
        grouped,
        "Append",
        options={},
        plan=plan,
        checkpoint_path=str(tmp_path / "checkpoint"),
    )

    assert len(captured) == len(plan.batches)
    assert sum(batch_df.count() for batch_df in captured) == plan.total_rows


def test_ingest_skips_planned_batches_without_groups(
    spark_fixture: SparkSession, mocker: MockerFixture
) -> None:
    grouped, plan = _group("predefined", _edges(spark_fixture), 4, "greedy")
    captured = _capture_neo4j_saves(mocker)

    with pytest.warns(UserWarning):
        ingest_spark_dataframe(
            grouped,
            "Append",
            options={},
            on_null_batch="skip",
            plan=IngestPlan(batches={**plan.batches, 7: []}, null_rows=plan.null_rows),
        )

    assert len(captured) == 1


@pytest.mark.parametrize(
    "strategy, group_keys, expected_scans",
    [
        # one counting pass, one write for the single predefined batch
        ("greedy", ["c"], 2),
        ("hash", ["c"], 2),
        # one counting pass, one write per batch (3 groups -> 3 batches)
        ("greedy", ["s", "t"], 4),
        ("hash", ["s", "t"], 4),
    ],
)
def test_build_relationship_scans_the_input_once_per_pass(
    spark_fixture: SparkSession,
    strategy: str,
    group_keys: List[str],
    expected_scans: int,
) -> None:
    rows = 200
    scanned = spark_fixture.sparkContext.accumulator(0)

    def make_row(i: int):
        scanned.add(1)
        return (str(i % 13), str(i % 13), str(i % 7 + 100))

    # an RDD source runs `make_row` exactly once per row every time the input is scanned
    sdf = spark_fixture.createDataFrame(
        spark_fixture.sparkContext.parallelize(range(rows), 4).map(make_row),
        "c string, s string, t string",
    )

    def evaluating_save(self, *args, **kwargs):
        self._df.rdd.foreach(lambda row: None)
        return MagicMock()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(DataFrameWriter, "save", evaluating_save)
        build_relationship(
            sdf,
            "REL",
            ":A",
            "c",
            ":B",
            "c",
            group_keys=group_keys,
            num_groups=3,
            max_serial=0,
            strategy=strategy,
        )

    assert scanned.value == expected_scans * rows
