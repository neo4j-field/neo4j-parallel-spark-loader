import pytest
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

from neo4j_parallel_spark_loader.utils.ingest import ingest_spark_dataframe


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

    assert "2 row(s) have a null `batch`" in str(e.value)


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

    with pytest.warns(UserWarning, match="1 row\\(s\\) have a null `batch`"):
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
