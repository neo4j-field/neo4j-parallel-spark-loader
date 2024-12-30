def _create_node_groupings(spark_dataframe, partition_col: str, num_groups: int) -> ...:
    """
    Create node groupings for parallel ingest into Neo4j.
    Add a `grouping` column to the Spark DataFrame identifying which group the row belongs in.

    Parameters
    ----------
    spark_dataframe : _type_
        _description_
    partition_col : str
        _description_
    num_groups : int
        _description_

    Returns
    -------
    ...
        _description_
    """
    ...

    # to create buckets
    # run over partition_col
    # group by and count
    # iterate through the values in max -> min order ex: [{key: Amazon, value_count: 100000}, ...]
    # find most-empty bucket (num_groups) and place value in it and increment bucket value by value_count
    # # track with 2 separate hash maps
