import pytest
from pyspark.sql import DataFrame

from parallel_spark_loader.utils.ingest import ingest_spark_dataframe


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
