from typing import Any, List, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes

TIME_LABELS = {
    "load_time": "Load Time (s)",
    "process_time": "Preprocess Time (s)",
    "total_time": "Total Time (s)"
}

def create_row_count_v_time_line_plot(dataframe: pd.DataFrame) -> Axes:
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


def create_num_groups_v_time_line_plot(dataframe: pd.DataFrame, time_col: str) -> Axes:
    sample_sizes = [10, 100, 1_000, 10_000, 100_000]
    hue_order = dataframe["num_groups"].unique()
    sns.set_theme()
    fig, axes = plt.subplots(1, 5, figsize=(15, 5), sharey=True)
    for idx, s in enumerate(sample_sizes):
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
        # if idx < len(sample_sizes) - 1:
        #     try:
        #         sns.move_legend(ax, loc="None")
        #     except Exception as e:
        #         continue
        if idx == 0:
            try:
                ax.set_ylabel(TIME_LABELS.get(time_col))
            except Exception as e:
                continue

        # axes[idx].set_xlabel("Graph Structure")
        # axes[idx].set_ylabel("Load Time (s)")
    fig.suptitle("Number of Groups Influence on Parallel Ingest Using Spark")
    # So far, nothing special except the managed prop_cycle. Now the trick:
    lines_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
    [ax.get_legend().remove() for ax in fig.axes]
    # print(lines_labels)
    
    # lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
    lines, labels = deduplicate_lines_and_labels(lines_labels)
    # print(lines)
    # print(labels)

    # Finally, the legend (that maybe you'll customize differently)
    fig.legend(lines, labels, loc="upper right", ncol=4)
    # sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    return axes


def deduplicate_lines_and_labels(lines_and_labels: List[Tuple[Any, float]]) -> Tuple[List[Any]]:
    final_labels: List[str] = []
    final_lines: List[str] = []

    for lines, labels in lines_and_labels:
        for idx in range(0, len(lines)):
            if labels[idx] not in final_labels:
                final_labels.append(labels[idx])
                final_lines.append(lines[idx])
    
    return (final_lines, final_labels)

def create_preprocess_v_load_time_chart(dataframe: pd.DataFrame) -> Axes:
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
    ax.set_title("Serial vs. Parallel Ingest Load & Preprocessing Times")
    sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
    return ax

        