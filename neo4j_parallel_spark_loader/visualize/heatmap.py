from typing import Any, Dict, List

import numpy as np
import seaborn as sns
from matplotlib.axes import Axes
from pyspark.sql import DataFrame


def _format_spark_dataframe_for_visualization(
    spark_dataframe: DataFrame,
) -> List[Dict[str, int]]:
    """
    Prepare a Pandas DataFrame to be displayed as a heatmap visualizing the group and batch counts.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame with `group` and `batch` columns.

    Returns
    -------
    List[Dict[str, int]]
        The Spark DataFrame contents processed and formatted as a list of dictionaries.
    """

    counts_sdf = spark_dataframe.groupBy("group", "batch").count()
    return [row.asDict() for row in counts_sdf.collect()]


def create_ingest_heatmap(
    spark_dataframe: DataFrame, title: str = "Parallel Ingest Heat Map"
) -> Axes:
    """
    Create the ingest heatmap from a list of dictionaries.
    This heatmap will display the groups on the y-axis and batches on the x-axis in sequential order.

    Parameters
    ----------
    spark_dataframe : DataFrame
        A Spark DataFrame with columns including 'group', 'batch' and 'count'
    title : str, optional
        A title for the visualization, by default "Parallel Ingest Heat Map"

    Returns
    -------
    Axes
        A Matplotlib Axes object for visualization.
    """

    data = _format_spark_dataframe_for_visualization(spark_dataframe=spark_dataframe)

    assert (
        set(data[0].keys()) == {"group", "batch", "count"}
    ), "Invalid keys detected in data. Dictionary keys must contain only 'group', 'batch' and 'count'."

    X_KEY = "batch"
    Y_KEY = "group"
    VALUE_KEY = "count"

    # Extract unique x and y values
    x_values = sorted(set(d[X_KEY] for d in data))
    y_values = sorted(set(d[Y_KEY] for d in data))

    # Create a 2D numpy array for the heatmap
    heatmap_data = np.zeros((len(y_values), len(x_values)))

    # Fill the array with values
    for item in data:
        x_idx = x_values.index(item[X_KEY])
        y_idx = y_values.index(item[Y_KEY])
        heatmap_data[y_idx, x_idx] = item[VALUE_KEY]

    ax = sns.heatmap(
        data=heatmap_data,
        annot=True,
        xticklabels=x_values,
        yticklabels=y_values,
        linewidths=0.5,
    )
    ax.set_xlabel("Batch")
    ax.set_ylabel("Group")
    ax.set_title(title)
    ax.invert_yaxis()
    return ax
