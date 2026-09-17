import json
import shutil

import pytest
from pyspark import StorageLevel
from pyspark.sql import functions as F

from neo4j_parallel_spark_loader.v2 import (
    _grouping_4_1_0 as direct,
    _grouping_fallback as fallback,
    _scheduling,
    ingest_spark_dataframe,
    load_staged_batches,
)


@pytest.mark.parametrize("backend", [fallback, direct], ids=["hash", "direct"])
@pytest.mark.parametrize("cache", [StorageLevel.NONE, StorageLevel.DISK_ONLY])
def test_resume_from_staging_without_original_source(
    v2_spark, tmp_path, monkeypatch, backend, cache
):
    if backend is direct and not hasattr(v2_spark.range(1), "repartitionById"):
        pytest.skip("Direct partition IDs require Spark 4.1+")
    source = tmp_path / "source"
    stage = tmp_path / "stage"
    v2_spark.range(64).selectExpr(
        "id AS source", "id % 7 AS target", "concat('payload-', id) AS payload"
    ).write.parquet(str(source))
    df = v2_spark.read.parquet(str(source))
    batches = backend.group_and_batch_spark_dataframe(
        df, "source", "target", 4, 321, staging_path=str(stage)
    )
    expected = [batch.collect() for batch in batches]
    assert len(expected) > 2
    manifest_bytes = (stage / "_neo4j_loader.json").read_bytes()

    # Lose the old caches, Python batch objects, and even the input dataset.
    v2_spark.catalog.clearCache()
    del batches, df
    shutil.rmtree(source)

    def fail_rebuilding(*args, **kwargs):
        raise AssertionError("Reloading must not regroup or eagerly scan batch rows")

    session = v2_spark.newSession()
    with monkeypatch.context() as patch:
        patch.setattr(_scheduling, "_rank_groups", fail_rebuilding)
        patch.setattr(fallback, "_partition_keys", fail_rebuilding)
        dataframe_class = type(session.range(1))
        patch.setattr(dataframe_class, "collect", fail_rebuilding)
        patch.setattr(dataframe_class, "count", fail_rebuilding)
        restored = load_staged_batches(session, str(stage), cache=cache)

    assert (stage / "_neo4j_loader.json").read_bytes() == manifest_bytes
    assert len(restored) == len(expected)
    assert all(batch.storageLevel == cache for batch in restored)
    assert all(
        batch.schema["batch"].metadata == {"neo4j_batch_size": 321}
        for batch in restored
    )
    written = []

    def consume_batch(writer, *args, **kwargs):
        rows = writer._df.withColumn("pid", F.spark_partition_id()).collect()
        assert stage.exists()
        assert len({r.batch for r in rows}) == 1
        assert len({r.pid for r in rows}) == len({r.group for r in rows})
        if backend is direct:
            assert all(r.pid == r.groupKey for r in rows)
        written.append({tuple(r)[:-1] for r in rows})

    monkeypatch.setattr(type(restored[0].write), "save", consume_batch)
    try:
        ingest_spark_dataframe(restored, "Overwrite", resume_from=3, unpersist=False)
        assert written == [{tuple(r) for r in rows} for rows in expected[2:]]
        assert stage.exists()
        written.clear()
        ingest_spark_dataframe(restored, "Overwrite", resume_from=3)
        assert written == [{tuple(r) for r in rows} for rows in expected[2:]]
        assert not stage.exists()
    finally:
        session.catalog.clearCache()


@pytest.mark.parametrize("manifest", [None, "not JSON", {"version": 99}])
def test_invalid_staging_is_rejected_without_deleting_files(
    v2_spark, tmp_path, manifest
):
    marker = tmp_path / "keep.txt"
    marker.write_text("existing files")
    if manifest is not None:
        (tmp_path / "_neo4j_loader.json").write_text(
            manifest if isinstance(manifest, str) else json.dumps(manifest)
        )
    with pytest.raises(ValueError):
        load_staged_batches(v2_spark, str(tmp_path))
    assert marker.read_text() == "existing files"


def test_direct_staging_rejects_runtime_without_direct_partitioning(v2_spark, tmp_path):
    stage = tmp_path / "stage"
    df = v2_spark.range(4).selectExpr("id AS source", "id AS target")
    grouped = direct._create_node_groupings_v2(df, "source", "target", 4, 123)
    grouped.write.partitionBy("batch").parquet(str(stage))
    owner = _scheduling._StagingDirectory(v2_spark, str(stage))
    from pyspark.sql import Row

    owner.write_manifest(
        grouped.schema, [Row(batch="0", partition_count=1, use_group=0)], direct=True
    )
    if hasattr(v2_spark.range(1), "repartitionById"):
        pytest.skip("This runtime supports direct partition IDs")
    with pytest.raises(ValueError, match="Spark 4.1"):
        load_staged_batches(v2_spark, str(stage))
    assert (stage / "_neo4j_loader.json").exists()
