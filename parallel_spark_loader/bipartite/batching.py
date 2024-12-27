
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