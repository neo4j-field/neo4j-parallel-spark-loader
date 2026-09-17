from collections import Counter, defaultdict
from itertools import product
from unittest.mock import Mock, call

import pytest
from pyspark import StorageLevel
from pyspark.serializers import CloudPickleSerializer
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType

from neo4j_parallel_spark_loader.v2 import (
    _grouping_4_1_0 as direct,
)
from neo4j_parallel_spark_loader.v2 import (
    _grouping_fallback as fallback,
)
from neo4j_parallel_spark_loader.v2 import (
    grouping,
)
from neo4j_parallel_spark_loader.v2 import _scheduling
from neo4j_parallel_spark_loader.v2.shipping import ingest_spark_dataframe


@pytest.fixture(params=[fallback, direct], ids=["fallback", "spark_4_1"])
def backend(request):
    return request.param


@pytest.mark.parametrize(
    "num_groups,batch_size", [(0, 100), (-1, 100), (4, 0), (4, -1)]
)
@pytest.mark.parametrize("module", [grouping, fallback, direct])
def test_rejects_nonpositive_sizes(module, num_groups, batch_size):
    name = "num_groups" if num_groups <= 0 else "batch_size"
    with pytest.raises(ValueError, match=f"{name} must be positive"):
        module.group_and_batch_spark_dataframe(
            None, "source", "target", num_groups, batch_size
        )


@pytest.mark.parametrize(
    "version,module",
    [
        ("3.4.0", fallback),
        ("3.5.6", fallback),
        ("4.0.1", fallback),
        ("4.1.0", direct),
        ("4.2.0", direct),
        ("4.10.0", direct),
        ("5.0.0", direct),
    ],
)
@pytest.mark.parametrize("staging_path", [None, "s3a://staging/unique-run"])
def test_routes_by_spark_version(monkeypatch, version, module, staging_path):
    df = Mock()
    df.sparkSession.version = version
    create = Mock()
    repartition = Mock()
    monkeypatch.setattr(module, "_create_node_groupings_v2", create)
    monkeypatch.setattr(module, "_apply_repartitioning", repartition)

    result = grouping.group_and_batch_spark_dataframe(
        df,
        "from_id",
        "to_id",
        7,
        123,
        StorageLevel.DISK_ONLY,
        staging_path=staging_path,
    )

    create.assert_called_once_with(
        df=df, source_col="from_id", target_col="to_id", num_groups=7, batch_size=123
    )
    repartition.assert_called_once_with(
        create.return_value, cache=StorageLevel.DISK_ONLY, staging_path=staging_path
    )
    assert result is repartition.return_value


@pytest.mark.parametrize("num_groups", [1, 4, 5])
def test_schedule_preserves_rows_and_prevents_endpoint_collisions(
    v2_spark, backend, num_groups
):
    # Every directed pair includes reverse edges and self-loops; duplicate rows
    # must survive scheduling too. Enough IDs exercise every endpoint bucket.
    data = [(a, b, f"{a}:{b}") for a, b in product(range(16), repeat=2)]
    data.append(data[1])
    df = v2_spark.createDataFrame(data, "source long, target long, payload string")
    df = (
        df.withColumn("group", F.lit("stale"))
        .withColumn("batch", F.lit("stale"))
        .withColumn("groupKey", F.lit(-1))
    )
    grouped = backend._create_node_groupings_v2(df, "source", "target", num_groups, 321)
    rows = grouped.collect()

    assert Counter((r.source, r.target, r.payload) for r in rows) == Counter(data)
    assert grouped.schema["batch"].metadata == {"neo4j_batch_size": 321}
    assert isinstance(grouped.schema["batch"].dataType, StringType)
    assert isinstance(grouped.schema["group"].dataType, StringType)
    assert isinstance(
        grouped.schema["groupKey"].dataType,
        LongType if backend is fallback else IntegerType,
    )
    assert len(grouped.columns) == len(set(grouped.columns))

    edges = {(r.source, r.target): (r.group, r.batch, r.groupKey) for r in rows}
    schedules = defaultdict(dict)
    for row in rows:
        assert edges[row.source, row.target] == edges[row.target, row.source]
        assert row.groupKey is not None
        schedules[row.batch][row.group] = row.groupKey
    # Complete bucket-pair coverage keeps the collision check non-vacuous.
    assert sum(map(len, schedules.values())) == num_groups * (num_groups + 1) // 2
    for groups in schedules.values():
        used = set()
        for group in groups:
            low, high = map(int, group.split("--"))
            assert 0 <= low <= high < num_groups
            endpoints = {low, high}
            assert used.isdisjoint(endpoints)
            used.update(endpoints)
        assert len(set(groups.values())) == len(groups)
        if backend is direct:
            assert [groups[g] for g in sorted(groups)] == list(range(len(groups)))

    reordered = backend._create_node_groupings_v2(
        df.orderBy(F.desc("payload")), "source", "target", num_groups, 321
    )
    assert {(r.group, r.batch, r.groupKey) for r in reordered.collect()} == {
        (r.group, r.batch, r.groupKey) for r in rows
    }


def test_batches_put_each_group_in_its_own_populated_partition(v2_spark, backend):
    if backend is direct and not hasattr(v2_spark.range(1), "repartitionById"):
        pytest.skip("Direct partition IDs require Spark 4.1+")
    data = list(product(range(12), repeat=2))
    df = v2_spark.createDataFrame(data, "source long, target long")
    batches = backend.group_and_batch_spark_dataframe(
        df, "source", "target", 4, 456, StorageLevel.DISK_ONLY
    )
    actual = []
    batch_ids = []
    try:
        for batch in batches:
            assert batch.storageLevel == StorageLevel.DISK_ONLY
            assert batch.schema["batch"].metadata["neo4j_batch_size"] == 456
            rows = batch.withColumn("partition_id", F.spark_partition_id()).collect()
            ids = {row.batch for row in rows}
            assert len(ids) == 1
            batch_ids.extend(ids)
            partitions = defaultdict(set)
            group_partitions = defaultdict(set)
            for row in rows:
                partitions[row.partition_id].add(row.group)
                group_partitions[row.group].add(row.partition_id)
                actual.append((row.source, row.target))
                if backend is direct:
                    assert row.partition_id == row.groupKey
            assert set(partitions) == set(range(batch.rdd.getNumPartitions()))
            assert all(len(groups) == 1 for groups in partitions.values())
            assert all(len(ids) == 1 for ids in group_partitions.values())
        assert batch_ids == sorted(set(batch_ids))
        assert Counter(actual) == Counter(data)
    finally:
        v2_spark.catalog.clearCache()


def test_empty_input_has_schedule_schema_and_no_batches(v2_spark, backend):
    df = v2_spark.createDataFrame([], "source long, target long")
    grouped = backend._create_node_groupings_v2(df, "source", "target", 4, 100)
    assert grouped.collect() == []
    # Joining the schedule by group may move that column to the front.
    assert Counter(grouped.columns) == Counter(
        ["source", "target", "group", "batch", "groupKey"]
    )
    assert grouped.schema["batch"].metadata["neo4j_batch_size"] == 100
    assert backend._apply_repartitioning(grouped, StorageLevel.NONE) == []


@pytest.mark.parametrize("empty", [False, True])
def test_grouping_does_not_serialize_python_functions(
    v2_spark, monkeypatch, empty, backend
):
    if backend is direct and not hasattr(v2_spark.range(1), "repartitionById"):
        pytest.skip("Direct partition IDs require Spark 4.1+")

    def fail_serialization(*args, **kwargs):
        raise AssertionError("Grouping must not serialize Python functions")

    monkeypatch.setattr(CloudPickleSerializer, "dumps", fail_serialization)
    df = v2_spark.range(0 if empty else 32).selectExpr(
        "id AS source", "id % 7 AS target"
    )
    try:
        batches = backend.group_and_batch_spark_dataframe(
            df, "source", "target", 4, 123, StorageLevel.DISK_ONLY
        )
        assert sum(batch.count() for batch in batches) == (0 if empty else 32)
        for batch in batches:
            rows = batch.withColumn("partition_id", F.spark_partition_id()).collect()
            partitions = defaultdict(set)
            for row in rows:
                assert row.groupKey is not None
                partitions[row.partition_id].add(row.group)
            assert all(len(groups) == 1 for groups in partitions.values())
            assert batch.schema["batch"].metadata == {"neo4j_batch_size": 123}
    finally:
        v2_spark.catalog.clearCache()


def test_group_identifiers_are_not_collected_on_driver(v2_spark, monkeypatch, backend):
    dataframe_class = type(v2_spark.range(1))
    original = dataframe_class.collect
    collected_columns = []

    def collect_counts_only(df):
        collected_columns.append(df.columns)
        assert "group" not in df.columns
        assert "groups" not in df.columns
        return original(df)

    df = v2_spark.range(128).selectExpr("id AS source", "id % 13 AS target")
    with monkeypatch.context() as patch:
        patch.setattr(dataframe_class, "collect", collect_counts_only)
        grouped = backend._create_node_groupings_v2(df, "source", "target", 20, 123)
    assert collected_columns
    assert grouped.count() == 128


def test_fallback_bounds_driver_key_metadata(v2_spark, monkeypatch):
    df = v2_spark.sql("""
        SELECT * FROM VALUES ('0', '0--0'), ('1', '0--1'), ('1', '2--4')
        AS data(batch, group)
    """)
    schedule, counts = fallback._rank_groups(df)
    monkeypatch.setattr(fallback, "_MAX_PARTITION_KEYS", 1)
    search = Mock(wraps=fallback._partition_keys)
    monkeypatch.setattr(fallback, "_partition_keys", search)
    grouped = fallback._guarantee_group_distinct_partitions(
        df, schedule, counts, v2_spark
    )
    search.assert_called_once_with(v2_spark, 1)
    rows = grouped.collect()
    assert len(rows) == 3
    assert {row.groupKey for row in rows if row.batch == "0"} == {0}
    assert {row.groupKey for row in rows if row.batch == "1"} == {None}


@pytest.mark.parametrize("group_count,expected_broadcast", [(2, True), (3, False)])
def test_only_small_group_schedules_are_forced_to_broadcast(
    v2_spark, monkeypatch, group_count, expected_broadcast
):
    monkeypatch.setattr(_scheduling, "_BROADCAST_GROUP_LIMIT", 2)
    previous = v2_spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    v2_spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        df = v2_spark.range(3).selectExpr("cast(id as string) AS group")
        keys = df.withColumn("groupKey", F.col("group").cast("long"))
        joined = _scheduling._join_group_keys(df, keys, group_count)
        plan = joined._jdf.queryExecution().executedPlan().toString()
        assert ("BroadcastHashJoin" in plan) == expected_broadcast
        assert {(r.group, r.groupKey) for r in joined.collect()} == {
            (str(i), i) for i in range(3)
        }
    finally:
        v2_spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous)


@pytest.mark.parametrize(
    "cache", [StorageLevel.MEMORY_AND_DISK, StorageLevel.DISK_ONLY, StorageLevel.NONE]
)
def test_unstaged_preparation_honors_cache(v2_spark, monkeypatch, cache):
    grouped = v2_spark.createDataFrame(
        [("0", "a", 0), ("1", "b", 0)], ["batch", "group", "groupKey"]
    )
    build_batches = _scheduling._build_batches
    intermediates = []

    def inspect_intermediate(scheduled, *args, **kwargs):
        assert scheduled.storageLevel == cache
        intermediates.append(scheduled)
        return build_batches(scheduled, *args, **kwargs)

    monkeypatch.setattr(_scheduling, "_build_batches", inspect_intermediate)
    batches = _scheduling._repartition_batches(grouped, cache, direct=False)
    try:
        assert intermediates[0].storageLevel == StorageLevel.NONE
        assert len(batches) == 2
        assert all(batch.storageLevel == cache for batch in batches)
        assert [batch.collect()[0].group for batch in batches] == ["a", "b"]
    finally:
        for batch in batches:
            batch.unpersist(blocking=True)


@pytest.mark.parametrize("failure_stage", ["schedule", "materialize"])
def test_preparation_failure_releases_intermediate_and_batch_caches(
    v2_spark, monkeypatch, failure_stage
):
    df = v2_spark.range(16).selectExpr("id AS source", "id % 3 AS target")
    grouped = fallback._create_node_groupings_v2(df, "source", "target", 4, 123)
    persistent = v2_spark.sparkContext._jsc.sc().getPersistentRDDs
    before = persistent().size()
    method = "collect" if failure_stage == "schedule" else "count"
    dataframe_class = type(grouped)
    original = getattr(dataframe_class, method)

    def fail_after_materialization(df):
        original(df)
        raise RuntimeError("preparation failed")

    monkeypatch.setattr(dataframe_class, method, fail_after_materialization)
    with pytest.raises(RuntimeError, match="preparation failed"):
        fallback._apply_repartitioning(grouped)
    assert persistent().size() == before


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("cache", [StorageLevel.NONE, StorageLevel.DISK_ONLY])
def test_staging_preserves_rows_metadata_and_supports_recomputation(
    v2_spark, backend, tmp_path, empty, cache
):
    if backend is direct and not hasattr(v2_spark.range(1), "repartitionById"):
        pytest.skip("Direct partition IDs require Spark 4.1+")
    size = 0 if empty else 32
    df = v2_spark.range(size).selectExpr("id AS source", "id % 7 AS target")
    path = str(tmp_path / "batches")
    batches = backend.group_and_batch_spark_dataframe(
        df, "source", "target", 4, 123, cache, staging_path=path
    )
    try:
        assert sum(batch.count() for batch in batches) == size
        for batch in batches:
            assert batch.storageLevel == cache
            assert batch.schema["batch"].metadata == {"neo4j_batch_size": 123}
            assert isinstance(batch.schema["batch"].dataType, StringType)
            cached = batch.collect()
            batch.unpersist(blocking=True)
            assert Counter(batch.collect()) == Counter(cached)
            # After removing the cache, each read selects one batch directory.
            plan = batch._jdf.queryExecution().executedPlan().toString()
            assert "PartitionFilters:" in plan
            assert "(batch#" in plan
            partitions = defaultdict(set)
            for row in batch.withColumn("pid", F.spark_partition_id()).collect():
                partitions[row.pid].add(row.group)
            assert all(len(groups) == 1 for groups in partitions.values())
    finally:
        v2_spark.catalog.clearCache()


def test_staging_does_not_overwrite_existing_files(v2_spark, tmp_path):
    marker = tmp_path / "keep.txt"
    marker.write_text("existing data")
    df = v2_spark.range(1).selectExpr("id AS source", "id AS target")
    with pytest.raises(Exception, match="(?i)already exists"):
        fallback.group_and_batch_spark_dataframe(
            df, "source", "target", 1, staging_path=str(tmp_path)
        )
    assert marker.read_text() == "existing data"


@pytest.mark.parametrize("fail_write", [False, True])
def test_shipping_retains_then_deletes_staging(
    v2_spark, tmp_path, monkeypatch, fail_write
):
    path = tmp_path / "staging"
    df = v2_spark.range(24).selectExpr("id AS source", "id % 5 AS target")
    batches = fallback.group_and_batch_spark_dataframe(
        df, "source", "target", 4, staging_path=str(path)
    )
    saves = []

    def consume_batch(writer, *args, **kwargs):
        assert path.exists()
        saves.append(writer._df.count())

    monkeypatch.setattr(type(batches[0].write), "save", consume_batch)
    try:
        ingest_spark_dataframe(batches, "Overwrite", unpersist=False)
        assert sum(saves) == 24
        assert path.exists()
        assert all(batch.storageLevel != StorageLevel.NONE for batch in batches)
        if fail_write:

            def fail(*args, **kwargs):
                raise RuntimeError("write failed")

            monkeypatch.setattr(type(batches[0].write), "save", fail)
            with pytest.raises(RuntimeError, match="write failed"):
                ingest_spark_dataframe(batches, "Overwrite", unpersist=False)
            assert path.exists()
            with pytest.raises(RuntimeError, match="write failed"):
                ingest_spark_dataframe(batches, "Overwrite", resume_from=2)
        else:
            # A slice must not delete files still needed by other batches.
            ingest_spark_dataframe(batches[:1], "Overwrite")
            assert path.exists()
            ingest_spark_dataframe(batches[1:], "Overwrite")
            assert sum(saves) == 48
        assert not path.exists()
        assert all(batch.storageLevel == StorageLevel.NONE for batch in batches)
        # Repeated cleanup cannot delete a new run that reuses the same path.
        path.mkdir()
        marker = path / "new-run.txt"
        marker.write_text("keep")
        _scheduling._release_staging(batches)
        assert marker.read_text() == "keep"
    finally:
        v2_spark.catalog.clearCache()


def test_failed_staged_preparation_cleans_its_directory(
    v2_spark, tmp_path, monkeypatch
):
    path = tmp_path / "staging"
    df = v2_spark.range(4).selectExpr("id AS source", "id AS target")

    def fail_count(*args, **kwargs):
        raise RuntimeError("materialization failed")

    monkeypatch.setattr(type(df), "count", fail_count)
    with pytest.raises(RuntimeError, match="materialization failed"):
        fallback.group_and_batch_spark_dataframe(
            df, "source", "target", 4, staging_path=str(path)
        )
    assert not path.exists()


@pytest.mark.parametrize("count", [1, 2, 7, 16, 128])
def test_fallback_keys_cover_real_hash_partitions(v2_spark, count):
    keys = fallback._partition_keys(v2_spark, count)
    df = v2_spark.createDataFrame(list(enumerate(keys)), "expected int, key long")
    rows = (
        df.repartition(count, "key")
        .withColumn("actual", F.spark_partition_id())
        .collect()
    )
    assert len(keys) == count
    assert all(row.expected == row.actual for row in rows)


@pytest.mark.parametrize("count", [0, -1])
def test_fallback_keys_reject_nonpositive_partition_counts(count):
    with pytest.raises(ValueError, match="partition_count must be positive"):
        fallback._partition_keys(None, count)


def test_fallback_key_search_returns_none_when_candidates_cannot_cover_partitions(
    v2_spark, monkeypatch
):
    # Force every candidate into one partition to exercise search exhaustion.
    monkeypatch.setattr(fallback.F, "hash", lambda *_: F.lit(0))
    range_spy = Mock(wraps=v2_spark.range)
    monkeypatch.setattr(v2_spark, "range", range_spy)
    assert fallback._partition_keys(v2_spark, 2) is None
    assert range_spy.call_args_list == [call(34 * 2**attempt) for attempt in range(8)]


@pytest.mark.parametrize("count,initial_budget", [(1, 32), (16, 301), (128, 2670)])
def test_fallback_search_scales_budget_and_stops_when_coverage_is_complete(
    v2_spark, count, initial_budget
):
    # Keep Spark expressions real, but control scan results to exercise a retry
    # without relying on a chance hash collision. Return keys out of order.
    spark = Mock()
    collect = spark.range.return_value.select.return_value.groupBy.return_value.agg.return_value.collect
    collect.side_effect = [
        [],
        [{"partition": i, "key": 1000 + i} for i in reversed(range(count))],
    ]

    assert fallback._partition_keys(spark, count) == list(range(1000, 1000 + count))
    assert spark.range.call_args_list == [
        call(initial_budget),
        call(initial_budget * 2),
    ]
    assert collect.call_count == 2


@pytest.mark.parametrize("all_exhausted", [False, True])
def test_exhausted_search_partitions_by_group_and_preserves_successful_keys(
    v2_spark, monkeypatch, all_exhausted
):
    data = [
        ("0", "0--0", "a"),
        ("1", "0--1", "b"),
        ("1", "2--4", "c"),
        ("3", "0--3", "d"),
        ("3", "1--2", "e"),
        ("3", "1--2", "e"),
    ]
    df = v2_spark.createDataFrame(data, "batch string, group string, payload string")
    df = df.withMetadata("batch", {"neo4j_batch_size": 123})
    schedule, counts = fallback._rank_groups(df)
    search = Mock(
        side_effect=lambda session, count: None if all_exhausted or count == 2 else [0]
    )
    monkeypatch.setattr(fallback, "_partition_keys", search)
    grouped = fallback._guarantee_group_distinct_partitions(
        df, schedule, counts, v2_spark
    )
    assert isinstance(grouped.schema["groupKey"].dataType, LongType)
    # Both successful and exhausted searches are reused across equal-size batches.
    assert search.call_count == 2
    search.assert_any_call(v2_spark, 1)
    search.assert_any_call(v2_spark, 2)

    actual = []
    try:
        batches = fallback._apply_repartitioning(grouped, StorageLevel.DISK_ONLY)
        assert len(batches) == 3
        for batch in batches:
            rows = batch.withColumn("partition_id", F.spark_partition_id()).collect()
            batch_id = rows[0].batch
            uses_group = all_exhausted or batch_id != "0"
            partition_count = 1 if batch_id == "0" else 2
            assert batch.rdd.getNumPartitions() == partition_count
            assert batch.storageLevel == StorageLevel.DISK_ONLY
            assert batch.schema["batch"].metadata["neo4j_batch_size"] == 123
            assert {row.groupKey for row in rows} == ({None} if uses_group else {0})
            # Compare actual partition placement with Spark hashing the original
            # group string (or the long key on batches whose search succeeded).
            expected = {
                row.group: row.expected
                for row in batch.select(
                    "group",
                    F.pmod(
                        F.hash("group" if uses_group else "groupKey"),
                        F.lit(partition_count),
                    ).alias("expected"),
                ).collect()
            }
            assert all(row.partition_id == expected[row.group] for row in rows)
            actual.extend((row.batch, row.group, row.payload) for row in rows)
        assert Counter(actual) == Counter(data)
    finally:
        v2_spark.catalog.clearCache()
