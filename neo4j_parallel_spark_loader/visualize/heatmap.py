from typing import Dict, List

import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes
from pyspark.sql import DataFrame


def create_heatmap_dataframe(spark_dataframe: DataFrame) -> pd.DataFrame:
    """
    Prepare a Pandas DataFrame to be displayed as a heatmap visualizing the group and batch counts.

    Parameters
    ----------
    spark_dataframe : DataFrame
        The Spark DataFrame with `group` and `batch` columns.

    Returns
    -------
    pd.DataFrame
        The Pandas DataFrame to feed into Seaborn for visualization.
    """

    counts_sdf = spark_dataframe.groupBy("group", "batch").count()
    return counts_sdf.toPandas()


def create_ingest_heatmap(
    dataframe: pd.DataFrame, title: str = "Parallel Ingest Heat Map"
) -> Axes:
    assert set(dataframe.columns) == {"group", "batch", "count"}

    formatted_data = dataframe.pivot(index="group", columns="batch", values="count")

    ax = sns.heatmap(data=formatted_data, annot=True, linewidths=0.5)
    ax.set_title(title)
    ax.invert_yaxis()
    return ax
