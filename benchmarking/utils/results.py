import os
from timeit import timeit
from typing import Any, Callable, Dict, Literal, Optional

import pandas as pd
import toml
from pyspark.sql import DataFrame, SparkSession


def create_results_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "row_count",
            "graph_structure",
            "load_time",
            "process_time",
            "load_strategy",
            "num_groups",
            "available_cpus_per_node",
            "spark.jars.packages",
            "neo4j_parallel_spark_loader_version",
            "dataset_name",
            "environment",
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
    dataset_name: str = "unknown",
    static_columns: Dict[str, Any] = {},
) -> Dict[str, Any]:
    return {
        "row_count": row_count,
        "graph_structure": graph_structure,
        "load_time": load_time,
        "process_time": process_time,
        "total_time": load_time + process_time,
        "load_strategy": load_strategy,
        "num_groups": num_groups,
        "available_cpus_per_node": static_columns.get("available_cpus_per_node"),
        "spark.jars.packages": static_columns.get("spark.jars.packages"),
        "neo4j_parallel_spark_loader_version": static_columns.get(
            "neo4j_parallel_spark_loader_version"
        ),
        "environment": static_columns.get("environment", "unknown"),
        "dataset_name": dataset_name or "unknown",
    }


def generate_benchmark_results(
    spark_dataframe: DataFrame,
    ingest_function: Callable[[Any], Any],
    graph_structure: Literal["bipartite", "monopartite", "predefined_components"],
    load_strategy: Literal["parallel", "serial"],
    num_groups: Optional[int] = None,
    dataset_name: str = "unknown",
    static_columns: Dict[str, Any] = {},
) -> Dict[str, Any]:
    row_count = spark_dataframe.count()
    proc_time, load_time = ingest_function(spark_dataframe, num_groups)

    return create_row(
        row_count=row_count,
        graph_structure=graph_structure,
        load_strategy=load_strategy,
        load_time=load_time,
        process_time=proc_time,
        num_groups=num_groups or 1,
        dataset_name=dataset_name,
        static_columns=static_columns,
    )


def get_package_version() -> str:
    with open("../pyproject.toml", "r") as f:
        config = toml.load(f)
        return config["tool"]["poetry"]["version"]


def save_dataframe(
    dataframe: pd.DataFrame, ts: str, spark: Optional[SparkSession] = None
) -> None:
    version = "v" + get_package_version()

    path = f"./output/{version}"

    if not os.path.exists(path):
        os.makedirs(path)

    file_path = f"{path}/benchmark_results-{ts}.csv"
    dataframe.to_csv(file_path, index=False)

    print(file_path)
