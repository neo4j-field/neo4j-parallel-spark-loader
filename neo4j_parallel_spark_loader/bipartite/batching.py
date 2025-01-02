from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def create_ingest_batches_from_groups(spark_dataframe: DataFrame) -> DataFrame:
    """
    Create batches for ingest into Neo4j.
    Add a `batch` column to the Spark DataFrame identifying which batch the group in that row belongs to.
    Remove `source_group` and `target_group` columns.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame to operate on.

    Returns
    -------
    DataFrame
        The Spark DataFrame with `batch` column added.
    """

    # assert that source_group and target_group exist in the dataframe
    # assert that the column types for above are IntegerType()

    source_group_count = spark_dataframe.select("source_group").distinct().count()

    target_group_count = spark_dataframe.select("target_group").distinct().count()

    num_colors = max(source_group_count, target_group_count)

    spark_dataframe = spark_dataframe.withColumn(
        "batch", (col("source_group") + col("target_group")) % num_colors
    ).drop(spark_dataframe.source_group, spark_dataframe.target_group)

    return spark_dataframe
