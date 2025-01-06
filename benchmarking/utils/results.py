from timeit import timeit
from typing import Any, Callable, Dict, Literal, Optional

import pandas as pd
from pyspark.sql import DataFrame


def create_results_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "row_count",
            "graph_structure",
            "load_time",
            "load_strategy",
            "num_groups",
        ]
    )


def append_results_to_dataframe(
    target_dataframe: pd.DataFrame, new_data: Dict[str, Any]
) -> pd.DataFrame:
    return target_dataframe.append(new_data, ignore_index=True)


def create_row(
    row_count: int,
    graph_structure: Literal["bipartite", "monopartite", "predefined_components"],
    load_time: float,
    load_strategy: Literal["parallel", "serial"],
    num_groups: int,
) -> Dict[str, Any]:
    return {
        "row_count": row_count,
        "graph_structure": graph_structure,
        "load_time": load_time,
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
    if num_groups is not None:
        load_time = timeit(ingest_function(spark_dataframe, num_groups), number=1)
    else:
        load_time = timeit(ingest_function(spark_dataframe), number=1)

    return create_row(
        row_count=row_count,
        graph_structure=graph_structure,
        load_strategy=load_strategy,
        load_time=load_time,
        num_groups=num_groups or 1,
    )


def save_dataframe(dataframe: pd.DataFrame) -> None:
    dataframe.to_csv("output/benchmark_results.csv", index=False)
