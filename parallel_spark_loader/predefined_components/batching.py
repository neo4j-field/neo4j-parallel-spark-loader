from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def create_ingest_batches_from_groups(spark_dataframe: DataFrame) -> DataFrame:
    """
    Create batches for ingest into Neo4j.
    Add a `batch` column to the Spark DataFrame identifying which batch the group in that row belongs to.
    In the case of `predefined components` all groups will be in the same batch.

    Parameters
    ----------
    spark_dataframe : DataFrame
        _description_

    Returns
    -------
    DataFrame
        The Spark DataFrame with a `batch` column.
    """

    return spark_dataframe.withColumn("batch", lit(0))
