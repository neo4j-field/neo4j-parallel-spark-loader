def ingest_spark_dataframe_into_neo4j(spark_dataframe, spark_session) -> None:
    """
    Ingest the Spark DataFrame into Neo4j in parallel.
    DataFrame must contain `grouping` and `batch` columns.

    Parameters
    ----------
    spark_dataframe : _type_
        _description_
    spark_session : _type_
        _description_
    """
    ...
