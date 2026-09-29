from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, DataFrameWriter, SparkSession

from neo4j_parallel_spark_loader.utils.build_relationship import build_relationship

ROWS = 200


def _scan_counted_dataframe(spark: SparkSession, null_every: Optional[int] = None):
    """
    Return a DataFrame read from an RDD whose `map` runs once per row every time the input is
    scanned, and the accumulator counting those runs.
    """
    scanned = spark.sparkContext.accumulator(0)

    def make_row(i: int):
        scanned.add(1)
        c = None if null_every and i % null_every == 0 else str(i % 13)
        return (c, str(i % 13), str(i % 7 + 100))

    sdf = spark.createDataFrame(
        spark.sparkContext.parallelize(range(ROWS), 4).map(make_row),
        "c string, s string, t string",
    )
    return sdf, scanned


def _cache_is_empty(spark: SparkSession) -> bool:
    return spark._jsparkSession.sharedState().cacheManager().isEmpty()


@pytest.fixture
def evaluating_save(monkeypatch: pytest.MonkeyPatch) -> List[DataFrame]:
    """Replace the Neo4j write with one that fully evaluates the DataFrame being written."""
    written: List[DataFrame] = []

    def save(self, *args, **kwargs):
        self._df.rdd.foreach(lambda row: None)
        written.append(self._df)
        return MagicMock()

    monkeypatch.setattr(DataFrameWriter, "save", save)
    return written


def _build(sdf: DataFrame, **kwargs) -> None:
    build_relationship(sdf, "REL", ":A", "c", ":B", "c", num_groups=3, **kwargs)


@pytest.fixture(params=["persist", "path"])
def materialize(request, tmp_path: Path) -> str:
    return "persist" if request.param == "persist" else str(tmp_path / "materialized")


@pytest.mark.parametrize("strategy", ["greedy", "hash"])
@pytest.mark.parametrize("group_keys", [["c"], ["s", "t"]])
def test_materialize_scans_the_input_once(
    spark_fixture: SparkSession,
    evaluating_save: List[DataFrame],
    materialize: str,
    strategy: str,
    group_keys: List[str],
) -> None:
    sdf, scanned = _scan_counted_dataframe(spark_fixture)

    _build(
        sdf,
        group_keys=group_keys,
        max_serial=0,
        strategy=strategy,
        materialize=materialize,
    )

    assert scanned.value == ROWS
    # bipartite with 3 groups writes 3 batches; all of them read the materialized input
    assert len(evaluating_save) == (1 if group_keys == ["c"] else 3)


@pytest.mark.parametrize("group_keys", [["c"], None])
def test_materialize_scans_the_input_once_on_the_serial_path(
    spark_fixture: SparkSession,
    evaluating_save: List[DataFrame],
    materialize: str,
    group_keys: Optional[List[str]],
) -> None:
    sdf, scanned = _scan_counted_dataframe(spark_fixture)

    _build(sdf, group_keys=group_keys, max_serial=ROWS, materialize=materialize)

    assert scanned.value == ROWS
    assert len(evaluating_save) == 1


def test_without_materialize_the_input_is_scanned_per_pass(
    spark_fixture: SparkSession, evaluating_save: List[DataFrame]
) -> None:
    sdf, scanned = _scan_counted_dataframe(spark_fixture)

    _build(sdf, group_keys=["s", "t"], max_serial=0)

    # one grouping pass plus one per batch
    assert scanned.value == 4 * ROWS


def test_persist_is_released_after_the_load(
    spark_fixture: SparkSession, evaluating_save: List[DataFrame]
) -> None:
    spark_fixture.catalog.clearCache()
    sdf, _ = _scan_counted_dataframe(spark_fixture)

    _build(sdf, group_keys=["c"], max_serial=0, materialize="persist")

    assert _cache_is_empty(spark_fixture)


def test_persist_is_released_when_the_load_fails(
    spark_fixture: SparkSession, evaluating_save: List[DataFrame]
) -> None:
    spark_fixture.catalog.clearCache()
    sdf, _ = _scan_counted_dataframe(spark_fixture, null_every=10)

    with pytest.raises(ValueError, match="null `batch` or `group`"):
        _build(sdf, group_keys=["c"], max_serial=0, materialize="persist")

    assert _cache_is_empty(spark_fixture)
    assert evaluating_save == []


def test_materialize_path_writes_parquet_and_overwrites(
    spark_fixture: SparkSession, evaluating_save: List[DataFrame], tmp_path: Path
) -> None:
    path = tmp_path / "materialized"
    spark_fixture.range(5).write.parquet(str(path))
    sdf, _ = _scan_counted_dataframe(spark_fixture)

    _build(sdf, group_keys=["c"], max_serial=0, materialize=str(path))

    materialized = spark_fixture.read.parquet(str(path))
    assert materialized.columns == ["c", "s", "t"]
    assert materialized.count() == ROWS
    assert sum(df.count() for df in evaluating_save) == ROWS


def test_materialize_rejects_an_empty_path(spark_fixture: SparkSession) -> None:
    sdf, _ = _scan_counted_dataframe(spark_fixture)

    with pytest.raises(AssertionError, match="`materialize` must be"):
        _build(sdf, group_keys=["c"], max_serial=0, materialize="")
