from pyspark.sql import SparkSession
import warnings

def verify_spark_version(spark_session: SparkSession) -> None:
    """
    Verify that Spark is a compatible version.
    This package was built and tested with Spark 3.4.0.
    It may work with other versions, but it is not guaranteed.
    """

    if spark_session.version < "3.4.0": warnings.warn("It is recommended to use Spark version >= 3.4.0")
