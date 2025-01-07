import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes


def create_row_count_v_time_line_plot(dataframe: pd.DataFrame) -> Axes:
    ax = sns.lineplot(
        data=dataframe,
        x="row_count",
        y="load_time",
        hue="graph_structure",
        style="load_strategy",
    )
    ax.set_xlabel("Row Count")
    ax.set_ylabel("Load Time (s)")
    ax.set_title("Serial vs. Parallel Ingest Using Spark")
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    plt.xscale("log")
    return ax


def create_num_groups_v_time_line_plot(dataframe: pd.DataFrame) -> Axes:
    sample_sizes = [10, 100, 1_000, 10_000, 100_000]
    fig, axes = plt.subplots(1, 5, figsize=(15, 5))
    for idx, s in enumerate(sample_sizes):
        ax = sns.barplot(
            ax=axes[idx],
            data=dataframe[
                (dataframe["load_strategy"] == "parallel")
                & (dataframe["row_count"] / s < 1)
            ],
            x="graph_structure",
            y="load_time",
            hue="num_groups",
        )
        ax.set_title(str(s) + " Samples")
        ax.set_xticklabels(["bip", "mon", "pdc"])
        if idx < 4:
            try:
                sns.move_legend(ax, loc="None")
            except Exception as e:
                continue
        # axes[idx].set_xlabel("Graph Structure")
        # axes[idx].set_ylabel("Load Time (s)")
    fig.suptitle("Number of Groups Influence on Parallel Ingest Using Spark")
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    return axes
