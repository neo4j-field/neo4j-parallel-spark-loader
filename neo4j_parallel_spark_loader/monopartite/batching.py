from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, lit, when, least, greatest, format_string


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
        The Spark DataFrame with `batch` column.
    """

    spark: SparkSession = spark_dataframe.sparkSession

    group_count = (
        spark_dataframe.select("source_group").withColumnRenamed("source_group", "group")
        .union(spark_dataframe.select("target_group").withColumnRenamed("target_group", "group"))
        .distinct()
        .count()
    )

    coloring = color_complete_graph_with_self_loops(group_count)

    coloring_data = [(f"{k[0]} -- {k[1]}", v) for k, v in coloring.items()]

    # Create a DataFrame from the coloring dictionary
    coloring_df = spark.createDataFrame(coloring_data, ["group", "batch"])

    # Join the DataFrames
    result_df = spark_dataframe.join(
        other=broadcast(coloring_df),
        on=(spark_dataframe.group == coloring_df.group),
        how="left",  # Use left join to keep all records from spark_dataframe
    ).drop(
        coloring_df.group
    )

    # Regroup self loops for even group_count
    if group_count % 2 == 0:
        self_loop_pair = (col("source_group") + lit(group_count//2)) % lit(group_count)
    
        # Create the new group string for self-loops
        self_loop_string = format_string(
            "self-loops %d and %d",
            least(col("source_group"), self_loop_pair),
            greatest(col("source_group"), self_loop_pair)
        )
    
        # Apply the transformation only when source_group equals target_group
        result_df = result_df.withColumn(
            "group",
            when(col("source_group") == col("target_group"), self_loop_string)
            .otherwise(col("group"))
        )

    result_df = result_df.drop(result_df.source_group, result_df.target_group)

    return result_df


def color_complete_graph_with_self_loops(n: int) -> Dict[Tuple[int], int]:
    """
    Colors the edges of a complete graph with self loops using a rotating pattern.

    Parameters
    ----------
    n : int
        Number of vertices in the graph

    Returns
    -------
    Dict[Tuple[int], int]
        Dictionary mapping edges (tuple) to colors (int)
    """

    edge_colors = {}
    current_color = 0

    # Function to add edge with color (handling both orientations)
    def _add_edge_color(v1: int, v2: int, color: int) -> None:
        """Assign a color to an edge - undirected."""
        edge_colors[(min(v1, v2), max(v1, v2))] = color

    def _step_through_edges(v1: int, v2: int, number_of_steps: int, color: int) -> None:
        """
        Assign a color to a group of edges (the number of steps = number of edges) where neither the
        source nor target node are associated with that color already.
        """

        for i in range(number_of_steps):
            vertex1 = (v1 - i) % n
            vertex2 = (v2 + i) % n
            if vertex1 < vertex2:
                min_vertex, max_vertex = vertex1, vertex2
            else:
                min_vertex, max_vertex = vertex2, vertex1
            if (min_vertex, max_vertex) not in edge_colors:
                _add_edge_color(min_vertex, max_vertex, color)

    # even number of nodes
    if n % 2 == 0:
        # Color odd-distance edges
        for start in range(n // 2):
            v1 = start
            v2 = start + 1
            _step_through_edges(v1, v2, n // 2, current_color)
            current_color += 1
        # Color even-distance edges
        for start in range((n) // 2):
            v1 = start
            v2 = start
            _step_through_edges(v1, v2, (n // 2) + 1, current_color)
            current_color += 1
            

    # odd number of nodes
    else:
        # color even and odd distance edges
        for start in range(n):
            v1 = start
            v2 = start
            _step_through_edges(v1, v2, (n + 1) // 2, current_color)
            current_color += 1

    return edge_colors
