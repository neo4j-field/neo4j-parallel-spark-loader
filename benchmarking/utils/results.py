import os
from timeit import timeit
from typing import Any, Callable, Dict, Literal, Optional

import pandas as pd
import toml
from pyspark.sql import DataFrame


def create_results_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "row_count",
            "graph_structure",
            "load_time",
            "process_time",
            "load_strategy",
            "num_groups",
        ]
    )


def append_results_to_dataframe(
    target_dataframe: pd.DataFrame, new_data: Dict[str, Any]
) -> pd.DataFrame:
    return pd.concat([target_dataframe, pd.DataFrame([new_data])], ignore_index=False)


def create_row(
    row_count: int,
    graph_structure: Literal["bipartite", "monopartite", "predefined_components"],
    load_time: float,
    process_time: float,
    load_strategy: Literal["parallel", "serial"],
    num_groups: int,
) -> Dict[str, Any]:
    return {
        "row_count": row_count,
        "graph_structure": graph_structure,
        "load_time": load_time,
        "process_time": process_time,
        "total_time": load_time + process_time,
        "load_strategy": load_strategy,
        "num_groups": num_groups,
    }


def generate_benchmark_results(
    spark_dataframe: DataFrame,
    ingest_function: Callable[[Any], Any],
    graph_structure: Literal["bipartite", "monopartite", "predefined_components"],
    load_strategy: Literal["parallel", "serial"],
    num_groups: Optional[int] = None,
) -> Dict[str, Any]:
    row_count = spark_dataframe.count()
    # if num_groups is not None:
    proc_time, load_time = ingest_function(spark_dataframe, num_groups)
    # else:
    #     load_time = timeit(lambda: ingest_function(spark_dataframe), number=1)

    return create_row(
        row_count=row_count,
        graph_structure=graph_structure,
        load_strategy=load_strategy,
        load_time=load_time,
        process_time=proc_time,
        num_groups=num_groups or 1,
    )


def _get_package_version() -> str:
    with open("./pyproject.toml", "r") as f:
        config = toml.load(f)
        return config["tool"]["poetry"]["version"]


def save_dataframe(dataframe: pd.DataFrame, ts: str) -> None:
    version = "v" + _get_package_version()
    path = f"benchmarking/output/{version}"

    if not os.path.exists(path):
        os.makedirs(path)

    dataframe.to_csv(f"{path}/benchmark_results-{ts}.csv", index=False)
