
def _create_node_groupings(spark_dataframe, source_col: str, target_col: str, num_groups: int) -> ...:
    """
    Create node groupings for parallel ingest into Neo4j.
    Add a `grouping` column to the Spark DataFrame identifying which group the row belongs in.

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

def _create_value_count_column(spark_dataframe, source_col: str, target_col: str, num_groups: int) -> ...:
    """
    Create `source_value_count` and `target_value_count` to inform node groupings.
    Add `source_value_count` and `target_value_count` columns counting the number of times a value appears in either the `source_col` or `target_col` column respectively.

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