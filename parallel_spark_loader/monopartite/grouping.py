
def _create_node_groupings(spark_dataframe, source_col: str, target_col: str, num_groups: int) -> ...:
    """
    Create node groupings for parallel ingest into Neo4j.
    The input DataFrame must have a `value_count` column.
    Add a `source_group` and `target_group` column to the Spark DataFrame identifying which group the row belongs in.

    Parameters
    ----------
    spark_dataframe : _type_
        _description_
    source_col : str
        _description_
    target_col : str
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
    # stack source and target
    # group by and count
    # iterate through the values in max -> min order ex: [{key: Amazon, value_count: 100000}, ...]
    # find most-empty bucket (num_groups) and place value in it and increment bucket value by value_count
    # # track with 2 separate hash maps


def _create_value_count_column(spark_dataframe, source_col: str, target_col: str, num_groups: int) -> ...:
    """
    Create `value_count` to inform node groupings.
    Add a `value_count` column counting the number of times a value appears in either the `source_col` or `target_col` column.

    Parameters
    ----------
    spark_dataframe : _type_
        _description_
    source_col : str
        _description_
    target_col : str
        _description_
    num_groups : int
        _description_

    Returns
    -------
    ...
        _description_
    """
    ...