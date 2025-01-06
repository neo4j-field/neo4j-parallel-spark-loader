from pyspark.sql import SparkSession


def verify_spark_version(spark_session: SparkSession) -> None:
    """
    Verify that Spark is a compatible version.
    This package requires Spark >= 3.4.0
    """

    assert spark_session.version >= "3.4.0", "Installed Spark version must be >= 3.4.0"
