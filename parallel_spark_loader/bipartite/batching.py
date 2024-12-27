
from pyspark.sql.functions import col

def create_ingest_batches_from_groups(spark_dataframe) -> ...:
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
    ...
        _description_
    """
    ...

    source_group_count = (spark_dataframe.select('source_group')
                          .distinct()
                          .count())
    
    target_group_count = (spark_dataframe.select('target_group')
                          .distinct()
                          .count())
    
    num_colors = max(source_group_count, target_group_count)

    spark_data_frame = spark_data_frame.withColumn(
        'batch', 
        (col('source_group') + col('target_group')) % num_colors
        )
    
    return spark_data_frame