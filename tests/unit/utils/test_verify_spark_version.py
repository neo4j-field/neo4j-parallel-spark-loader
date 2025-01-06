from unittest.mock import MagicMock

import pytest

from neo4j_parallel_spark_loader.utils.verify_spark import verify_spark_version


def test_verify_spark_version_incompatible_version(
    mock_spark_session_incompatible: MagicMock,
) -> None:
    with pytest.raises(AssertionError):
        verify_spark_version(spark_session=mock_spark_session_incompatible)


def test_verify_spark_version_compatible_version(mock_spark_session: MagicMock) -> None:
    verify_spark_version(spark_session=mock_spark_session)
