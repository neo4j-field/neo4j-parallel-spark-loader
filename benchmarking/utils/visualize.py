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

SAMPLE_SIZES = [1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000]
SAMPLE_LABELS = ["1M", "2M", "3M", "4M", "5M", "6M"]


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
    # plt.xscale("log")
    tick_locations = SAMPLE_SIZES
    tick_labels = SAMPLE_LABELS
    plt.xticks(tick_locations, tick_labels, rotation=45)
    return ax


def create_num_groups_v_time_bar_plot(dataframe: pd.DataFrame, time_col: str) -> Axes:
    hue_order = (
        dataframe[dataframe["load_strategy"] == "parallel"]["num_groups"]
        .drop_duplicates()
        .sort_values()
    )
    # Define the mapping for abbreviated labels
    label_map = {"predefined_components": "pdc", "bipartite": "bp", "monopartite": "mp"}
    sns.set_theme()
    row_counts = dataframe["row_count"].drop_duplicates().sort_values()
    fig, axes = plt.subplots(1, len(row_counts) - 3, figsize=(15, 5), sharey=True)
    for idx, s in enumerate(row_counts[:-3]):
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
        ax.set_xlabel("Graph Structure")
        ax.set_xticklabels(
            [label_map[label.get_text()] for label in ax.get_xticklabels()]
        )

        if idx == 0:
            try:
                ax.set_ylabel(TIME_LABELS.get(time_col))
            except Exception as e:
                continue
        else:
            # Remove ylabel for all other plots
            ax.set_ylabel("")

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
                # & (dataframe["num_groups"].isin([1, 3]))
            ],
            x="row_count",
            y=s,
            hue="load_strategy",
            style="num_groups",
        )
        # ax.set_xscale("log")
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
        # ax.set_xscale("log")
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
        # ax.set_xscale("log")
        ax.set_xticks(SAMPLE_SIZES)
        ax.set_xticklabels(SAMPLE_LABELS)

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
