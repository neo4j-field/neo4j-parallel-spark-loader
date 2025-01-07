import warnings
from datetime import datetime
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from tqdm import tqdm

from benchmarking.utils.database import *
from benchmarking.utils.healthcheck import healthcheck
from benchmarking.utils.neo4j_driver import create_neo4j_driver
from benchmarking.utils.results import (
    append_results_to_dataframe,
    create_results_dataframe,
    generate_benchmark_results,
    save_dataframe,
)
from benchmarking.utils.spark import create_spark_session

warnings.simplefilter(action="ignore", category=FutureWarning)


def load_data_into_spark_dataframe(
    spark_session: SparkSession,
    category: Literal["bipartite", "monopartite", "predefined_components"],
) -> DataFrame:
    file_path = f"benchmarking/data/{category}_data.csv"

    return spark_session.read.option("header", True).csv(file_path)


if __name__ == "__main__":
    spark_session: SparkSession = create_spark_session()
    neo4j_driver = create_neo4j_driver()

    bp_sdf = load_data_into_spark_dataframe(spark_session, "bipartite")
    mp_sdf = load_data_into_spark_dataframe(spark_session, "monopartite")
    pc_sdf = load_data_into_spark_dataframe(spark_session, "predefined_components")

    ingest_functions = {
        "bipartite": {
            "serial": load_bipartite_relationships_in_serial,
            "parallel": load_bipartite_relationships_in_parallel,
            "nodes": load_bipartite_nodes,
        },
        "monopartite": {
            "serial": load_monopartite_relationships_in_serial,
            "parallel": load_monopartite_relationships_in_parallel,
            "nodes": load_monopartite_nodes,
        },
        "predefined_components": {
            "serial": load_bipartite_relationships_in_serial,  # using bipartite data
            "parallel": load_predefined_components_relationships_in_parallel,
            "nodes": load_bipartite_nodes,
        },
    }

    sample_fractions = [0.0001, 0.001, 0.01, 0.1, 1.0]

    sdfs = {0: bp_sdf, 2: mp_sdf, 4: pc_sdf}

    unsampled_tasks = [
        {"graph_structure": "bipartite", "load_strategy": "serial", "num_groups": None},
        {"graph_structure": "bipartite", "load_strategy": "parallel", "num_groups": 3},
        {
            "graph_structure": "monopartite",
            "load_strategy": "serial",
            "num_groups": None,
        },
        {
            "graph_structure": "monopartite",
            "load_strategy": "parallel",
            "num_groups": 5,
        },
        {
            "graph_structure": "predefined_components",
            "load_strategy": "serial",
            "num_groups": None,
        },
        {
            "graph_structure": "predefined_components",
            "load_strategy": "parallel",
            "num_groups": 3,
        },
    ]

    results_df = create_results_dataframe()

    ts = str(datetime.now())

    for idx in tqdm(range(0, len(unsampled_tasks), 2), desc="graph structure"):
        print(unsampled_tasks[idx].get("graph_structure"))
        for s in tqdm(sample_fractions, desc="sample sizes"):
            sampled_sdf = sdfs.get(idx).sample(s)

            # create constraints
            # create_constraints(spark_session=spark_session)

            graph_structure = unsampled_tasks[idx].get("graph_structure")

            # load nodes
            fn = ingest_functions.get(graph_structure).get("nodes")
            fn(sampled_sdf)

            # load relationships
            load_strategy = unsampled_tasks[idx].get("load_strategy")
            num_groups = unsampled_tasks[idx].get("num_groups")
            # idx
            results_row = generate_benchmark_results(
                spark_dataframe=sampled_sdf,
                graph_structure=graph_structure,
                ingest_function=ingest_functions.get(graph_structure).get(
                    load_strategy
                ),
                load_strategy=load_strategy,
                num_groups=num_groups if load_strategy == "parallel" else None,
            )
            results_df = append_results_to_dataframe(results_df, results_row)

            save_dataframe(results_df, ts)

            # clean up relationships
            delete_relationships(spark_session=spark_session)

            # idx + 1
            load_strategy = unsampled_tasks[idx + 1].get("load_strategy")
            num_groups = unsampled_tasks[idx + 1].get("num_groups")
            results_row = generate_benchmark_results(
                spark_dataframe=sampled_sdf,
                graph_structure=graph_structure,
                ingest_function=ingest_functions.get(graph_structure).get(
                    load_strategy
                ),
                load_strategy=load_strategy,
                num_groups=num_groups if load_strategy == "parallel" else None,
            )
            results_df = append_results_to_dataframe(results_df, results_row)

            save_dataframe(results_df, ts)

            # refresh database
            restore_database(neo4j_driver=neo4j_driver)

            healthcheck(neo4j_driver=neo4j_driver)

    neo4j_driver.close()
