
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def create_ingest_batches_from_groups(spark_dataframe: DataFrame) -> DataFrame:
    """
    Create batches for ingest into Neo4j.
    Add a `batch` column to the Spark DataFrame identifying which batch the group in that row belongs to.
    In the case of `predefined components` all groups will be in the same batch.

    Parameters
    ----------
    spark_dataframe : _type_
        _description_

    Returns
    -------
    DataFrame
        _description_
    """
    
    # assert that source_group and target_group exist in the dataframe
    # assert that the column types for above are IntegerType()

    source_group_count = (spark_dataframe.select('source_group')
                          .distinct()
                          .count())
    
    target_group_count = (spark_dataframe.select('target_group')
                          .distinct()
                          .count())
    
    num_colors = max(source_group_count, target_group_count)

    spark_dataframe = spark_dataframe.withColumn(
        'batch', 
        (col('source_group') + col('target_group')) % num_colors
        )
    
    return spark_dataframe