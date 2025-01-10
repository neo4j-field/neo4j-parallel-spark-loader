from typing import Any, List, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes

TIME_LABELS = {
    "load_time": "Load Time (s)",
    "process_time": "Preprocess Time (s)",
    "total_time": "Total Time (s)",
}

TIME_TITLES = {
    "load_time": "Load Times",
    "process_time": "Preprocess Times",
    "total_time": "Total Times",
}

SAMPLE_SIZES = [10, 100, 1_000, 10_000, 100_000, 1_000_000]

def create_row_count_v_load_time_line_plot(dataframe: pd.DataFrame) -> Axes:
    sns.set_theme()
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


def create_num_groups_v_time_bar_plot(dataframe: pd.DataFrame, time_col: str) -> Axes:
    
    hue_order = dataframe["num_groups"].unique()
    sns.set_theme()
    fig, axes = plt.subplots(1, len(SAMPLE_SIZES), figsize=(15, 5), sharey=True)
    for idx, s in enumerate(SAMPLE_SIZES):
        ax = sns.barplot(
            ax=axes[idx],
            data=dataframe[
                (dataframe["load_strategy"] == "parallel")
                & (dataframe["row_count"] == s)
            ],
            x="graph_structure",
            y=time_col,
            hue="num_groups",
            hue_order=hue_order,
        )
        ax.set_title(str(s) + " Samples")
        ax.set_xticklabels(["bip", "mon", "pdc"])
        ax.set_xlabel("Graph Structure")

        if idx == 0:
            try:
                ax.set_ylabel(TIME_LABELS.get(time_col))
            except Exception as e:
                continue

    fig.suptitle("Number of Groups Influence on Parallel Ingest Using Spark")

    lines_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
    [ax.get_legend().remove() for ax in fig.axes]

    lines, labels = _deduplicate_lines_and_labels(lines_labels)

    fig.legend(lines, labels, loc="upper right", ncol=4)

    return axes


def _deduplicate_lines_and_labels(
    lines_and_labels: List[Tuple[Any, float]],
) -> Tuple[List[Any]]:
    final_labels: List[str] = []
    final_lines: List[str] = []

    for lines, labels in lines_and_labels:
        for idx in range(0, len(lines)):
            if labels[idx] not in final_labels:
                final_labels.append(labels[idx])
                final_lines.append(lines[idx])

    return (final_lines, final_labels)


def create_load_v_total_time_plot(dataframe: pd.DataFrame) -> Axes:
    sns.set_theme()
    ax = sns.scatterplot(
        data=dataframe,
        x="load_time",
        y="total_time",
        style="load_strategy",
        hue="graph_structure",
    )
    ax.set_xlabel("Load Time (s)")
    ax.set_ylabel("Total Time (s)")
    ax.set_title("Serial vs. Parallel Ingest Load & Total Times")
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    return ax


def create_time_v_row_count_for_graph_structure_line_plot(
    dataframe: pd.DataFrame, graph_structure: str
) -> Axes:
    times = ["process_time", "load_time", "total_time"]

    sns.set_theme()
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
    for idx, s in enumerate(times):
        ax = sns.lineplot(
            ax=axes[idx],
            data=dataframe[
                (dataframe["graph_structure"] == graph_structure)
                & (dataframe["num_groups"].isin([1, 3]))
            ],
            x="row_count",
            y=s,
            hue="load_strategy",
        )
        ax.set_xscale("log")
        sns.lineplot(
            ax=axes[idx],
            data=dataframe[
                (dataframe["graph_structure"] == graph_structure)
                & (dataframe["num_groups"].isin([1, 3]))
            ],
            x="row_count",
            y=s,
            hue="load_strategy",
        )
        ax.set_xscale("log")
        sns.lineplot(
            ax=axes[idx],
            data=dataframe[
                (dataframe["graph_structure"] == graph_structure)
                & (dataframe["num_groups"].isin([1, 3]))
            ],
            x="row_count",
            y=s,
            hue="load_strategy",
        )
        ax.set_title(TIME_TITLES.get(s))
        # ax.set_xticklabels(["Preprocess", "Load", "Total"])
        ax.set_xlabel("Row Count")
        ax.set_xscale("log")

        if idx == 0:
            try:
                ax.set_ylabel("Time (s)")
            except Exception as e:
                continue
    fig.suptitle(f"{graph_structure.capitalize()} Serial vs. Parallel Times")
    lines_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
    [ax.get_legend().remove() for ax in fig.axes]

    lines, labels = _deduplicate_lines_and_labels(lines_labels)

    fig.legend(lines, labels, loc="upper right", ncol=1)

    return axes
