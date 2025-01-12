from typing import Any, Dict, List
import numpy as np
import seaborn as sns
from matplotlib.axes import Axes
from pyspark.sql import DataFrame
import matplotlib.pyplot as plt

def _format_spark_dataframe_for_visualization(
    spark_dataframe: DataFrame,
) -> List[Dict[str, Any]]:
    """
    Prepare a Pandas DataFrame to be displayed as a heatmap visualizing the group and batch counts.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame with `group` and `batch` columns.

    Returns
    -------
    List[Dict[str, Any]]
        The Spark DataFrame contents processed and formatted as a list of dictionaries.
    """
    counts_sdf = spark_dataframe.groupBy("group", "batch").count()
    return [row.asDict() for row in counts_sdf.collect()]

def create_ingest_heatmap(
    spark_dataframe: DataFrame, title: str = "Parallel Ingest Heat Map", figsize=(8, 12)
) -> Axes:
    """
    Create the ingest heatmap from a list of dictionaries.
    This heatmap will display batches on the y-axis and group numbers on the x-axis.

    Parameters
    ----------
    spark_dataframe : DataFrame
        A Spark DataFrame with columns including 'group', 'batch' and 'count'
    title : str, optional
        A title for the visualization, by default "Parallel Ingest Heat Map"
    figsize : tuple, optional
        Figure size (width, height) in inches, by default (8, 12)

    Returns
    -------
    Axes
        A Matplotlib Axes object for visualization.
    """
    data = _format_spark_dataframe_for_visualization(spark_dataframe=spark_dataframe)

    assert (
        set(data[0].keys()) == {"group", "batch", "count"}
    ), "Invalid keys detected in data. Dictionary keys must contain only 'group', 'batch' and 'count'"

    # Create a dictionary to store group-to-number mapping for each batch
    batch_group_mappings = {}
    for d in data:
        batch = d["batch"]
        if batch not in batch_group_mappings:
            batch_group_mappings[batch] = {}
        batch_group_mappings[batch][d["group"]] = len(batch_group_mappings[batch]) + 1

    # Transform data with group numbers
    transformed_data = []
    for d in data:
        transformed_data.append({
            "batch": d["batch"],
            "group_num": batch_group_mappings[d["batch"]][d["group"]],
            "count": d["count"],
            "original_group": d["group"]
        })

    # Extract unique x and y values
    y_values = sorted(set(d["batch"] for d in transformed_data), reverse=True)
    x_values = sorted(set(d["group_num"] for d in transformed_data))

    # Create 2D numpy arrays for the heatmap
    heatmap_data = np.zeros((len(y_values), len(x_values)))
    annotation_labels = np.empty((len(y_values), len(x_values)), dtype=object)

    # Fill the arrays with values
    for item in transformed_data:
        y_idx = y_values.index(item["batch"])
        x_idx = x_values.index(item["group_num"])
        heatmap_data[y_idx, x_idx] = item["count"]
        # Create annotation with count and original group name
        annotation_labels[y_idx, x_idx] = f"{item['count']:,.0f}\n({item['original_group']})"

    # Create figure with specified size
    plt.figure(figsize=figsize)
    
    # Create heatmap
    ax = sns.heatmap(
        data=heatmap_data,
        annot=annotation_labels,
        fmt="",
        xticklabels=x_values,
        yticklabels=y_values,
        linewidths=0.5,
    )
    
    ax.set_xlabel("Group Number")
    ax.set_ylabel("Batch")
    ax.set_title(title)
    
    return ax