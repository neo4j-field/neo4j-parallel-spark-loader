
def create_ingest_batches_from_groups(spark_dataframe, spark) -> ...:
    """
    Create batches for ingest into Neo4j.
    Add a `batch` column to the Spark DataFrame identifying which batch the group in that row belongs to.

    Parameters
    ----------
    spark_dataframe : _type_
        _description_

    Returns
    -------
    ...
        _description_
    """

    group_count = (spark_dataframe.select('source_group')
                   .union(spark_dataframe.select('target_group'))
                   .distinct()
                   .count())
    
    coloring = color_complete_graph_with_self_loops(group_count)

    coloring_data = [(k[0], k[1], v) for k, v in coloring.items()]

    # Create a DataFrame from the coloring dictionary
    coloring_df = spark.createDataFrame(
        coloring_data,
        ["source_group", "target_group", "batch"]
    )

    # Join the DataFrames
    result_df = spark_dataframe.join(
        coloring_df,
        (spark_dataframe.source_group == coloring_df.source_group) &
        (spark_dataframe.target_group == coloring_df.target_group),
        "left"  # Use left join to keep all records from spark_dataframe
    )

    return result_df

def color_complete_graph_with_self_loops(n: int) -> dict:
    """
    Colors the edges of a complete graph with self loops using a rotating pattern.
    
    Args:
        n: Number of vertices in the graph
        
    Returns:
        Dictionary mapping edges (tuple) to colors (int)
    """
    edge_colors = {}
    current_color = 0
    
    # Function to add edge with color (handling both orientations)
    def add_edge_color(v1: int, v2: int, color: int):
        edge_colors[(min(v1, v2), max(v1, v2))] = color

    def step_through_edges(v1, v2, number_of_steps, color):
        for i in range(number_of_steps):
            vertex1 = (v1 - i) % n
            vertex2 = (v2 + i) % n
            if (min(vertex1, vertex2), max(vertex1, vertex2)) not in edge_colors:
                add_edge_color(vertex1, vertex2, color)
            else:
                print(f"{min(vertex1, vertex2), max(vertex1, vertex2)} is already colored")

    if n%2 == 0:
        # Color even-distance edges
        for start in range(n//2):
            v1 = start
            v2 = start
            step_through_edges(v1, v2, n//2+1, current_color)
            current_color += 1
        # Color odd-distance edges
        for start in range(n//2):
            v1 = start
            v2 = start + 1
            step_through_edges(v1, v2, n//2, current_color)
            current_color += 1

    else:
        for start in range(n):
            v1 = start
            v2 = start
            step_through_edges(v1, v2, n//2 + 1, current_color)
            current_color += 1
        
    return edge_colors

