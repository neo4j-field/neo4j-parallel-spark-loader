from unittest.mock import Mock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from neo4j_parallel_spark_loader.v2.shipping import ingest_spark_dataframe


def make_batch(metadata=None, columns=("batch", "group")):
    batch = Mock()
    batch.columns = list(columns)
    batch.schema = StructType([StructField(name, StringType(), metadata=metadata if name == "batch" else {}) for name in columns])
    batch.write.mode.return_value = batch.write
    batch.write.format.return_value = batch.write
    batch.write.options.return_value = batch.write
    return batch


@pytest.mark.parametrize("mode", ["Append", "Overwrite"])
@pytest.mark.parametrize(
    "metadata,options,expected",
    [
        ({}, {"query": "RETURN 1"}, {"query": "RETURN 1"}),
        (
            {"neo4j_batch_size": 123},
            {"query": "RETURN 1"},
            {"query": "RETURN 1", "batch.size": "123"},
        ),
        ({"neo4j_batch_size": 123}, {"batch.size": "999"}, {"batch.size": "999"}),
    ],
)
def test_writes_batches_serially_with_connector_options(mode, metadata, options, expected):
    batches = [make_batch(metadata), make_batch(metadata)]
    events = []
    for index, batch in enumerate(batches):
        batch.write.save.side_effect = lambda i=index: events.append(("save", i))
        batch.unpersist.side_effect = lambda *, blocking, i=index: events.append(("unpersist", i))
    original = dict(options)

    ingest_spark_dataframe(batches, mode, options)

    assert events == [("save", 0), ("unpersist", 0), ("save", 1), ("unpersist", 1)]
    assert options == original
    for batch in batches:
        batch.write.mode.assert_called_once_with(mode)
        batch.write.format.assert_called_once_with("org.neo4j.spark.DataSource")
        batch.write.options.assert_called_once_with(**expected)
        batch.write.save.assert_called_once_with()
        batch.unpersist.assert_called_once_with(blocking=False)


def test_default_options_do_not_leak_between_calls():
    first = make_batch({"neo4j_batch_size": 123})
    second = make_batch()
    ingest_spark_dataframe([first], "Append")
    ingest_spark_dataframe([second], "Append")
    first.write.options.assert_called_once_with(**{"batch.size": "123"})
    second.write.options.assert_called_once_with()


@pytest.mark.parametrize("mode", ["append", "overwrite", "Ignore", "", None])
def test_invalid_save_mode_is_rejected_before_writing(mode):
    batch = make_batch()
    with pytest.raises(ValueError, match="save_mode must be either 'Append' or 'Overwrite'"):
        ingest_spark_dataframe([batch], mode)
    batch.write.mode.assert_not_called()


@pytest.mark.parametrize("missing", ["batch", "group"])
def test_missing_required_column_is_rejected(missing):
    batch = make_batch(columns=[name for name in ("batch", "group") if name != missing])
    with pytest.raises(ValueError, match=f"Spark DataFrame must contain column `{missing}`"):
        ingest_spark_dataframe([batch], "Append")
    batch.write.mode.assert_not_called()


def test_empty_batches_are_a_noop():
    ingest_spark_dataframe([], "Append")


def test_failed_write_releases_current_batch_and_stops_shipping():
    batches = [make_batch(), make_batch(), make_batch()]
    failure = RuntimeError("write failed")
    batches[1].write.save.side_effect = failure
    with pytest.raises(RuntimeError, match="write failed") as exc:
        ingest_spark_dataframe(batches, "Append")
    assert exc.value is failure
    batches[0].write.save.assert_called_once_with()
    batches[0].unpersist.assert_called_once_with(blocking=False)
    batches[1].unpersist.assert_called_once_with(blocking=False)
    batches[2].write.mode.assert_not_called()


@pytest.mark.parametrize("missing", ["batch", "group"])
def test_validates_all_batches_before_first_write(missing):
    first = make_batch()
    invalid = make_batch(columns=[name for name in ("batch", "group") if name != missing])
    with pytest.raises(ValueError, match=f"Spark DataFrame must contain column `{missing}`"):
        ingest_spark_dataframe([first, invalid], "Append")
    first.write.mode.assert_not_called()
    invalid.write.mode.assert_not_called()


def test_writer_setup_failure_also_releases_batch():
    batch = make_batch()
    batch.write.options.side_effect = RuntimeError("invalid connector options")
    with pytest.raises(RuntimeError, match="invalid connector options"):
        ingest_spark_dataframe([batch], "Append")
    batch.unpersist.assert_called_once_with(blocking=False)
    batch.write.save.assert_not_called()
