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

def sample_spark_dataframe(spark_dataframe: DataFrame, desired_number: int) -> DataFrame:
    """Work-around for Spark's inaccurate sampling method."""
    
    fraction = desired_number / spark_dataframe.count() * 1.2

    return spark_dataframe.sample(False, fraction, seed=42).limit(desired_number)


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

    # sample_fractions = [0.0001, 0.001, 0.01, 0.1, 1.0]
    sample_sizes = [10, 100, 1_000, 10_000, 100_000]

    sdfs = {0: bp_sdf, 2: mp_sdf, 4: pc_sdf}

    SERIAL_GROUPS = [1]
    BIPARTITE_GROUPS = [2, 3]
    MONOPARTITE_GROUPS = [2, 5]
    PREDEFINED_COMPONENTS_GROUPS = [2, 3]

    unsampled_tasks = [
        {
            "graph_structure": "bipartite",
            "load_strategy": "serial",
            "num_groups": SERIAL_GROUPS,
        },
        {
            "graph_structure": "bipartite",
            "load_strategy": "parallel",
            "num_groups": BIPARTITE_GROUPS,
        },
        {
            "graph_structure": "monopartite",
            "load_strategy": "serial",
            "num_groups": SERIAL_GROUPS,
        },
        {
            "graph_structure": "monopartite",
            "load_strategy": "parallel",
            "num_groups": MONOPARTITE_GROUPS,
        },
        {
            "graph_structure": "predefined_components",
            "load_strategy": "serial",
            "num_groups": SERIAL_GROUPS,
        },
        {
            "graph_structure": "predefined_components",
            "load_strategy": "parallel",
            "num_groups": PREDEFINED_COMPONENTS_GROUPS,
        },
    ]

    # init new dataframe
    results_df = create_results_dataframe()

    # mark benchmarking start time
    ts = str(datetime.now())

    # wait for database to be online before running
    healthcheck(neo4j_driver=neo4j_driver)

    for idx in tqdm(range(0, len(unsampled_tasks), 2), desc="graph structure"):
        print(unsampled_tasks[idx].get("graph_structure"))
        for s in tqdm(sample_sizes, desc="sample sizes"):
            # sampled_sdf = sdfs.get(idx).sample(s)
            sampled_sdf: DataFrame = sample_spark_dataframe(sdfs.get(idx), s)

            # create constraints
            create_constraints(neo4j_driver=neo4j_driver)

            graph_structure = unsampled_tasks[idx].get("graph_structure")

            # load nodes
            fn = ingest_functions.get(graph_structure).get("nodes")
            fn(sampled_sdf)

            # load relationships
            load_strategy = unsampled_tasks[idx].get("load_strategy")
            num_groups = unsampled_tasks[idx].get("num_groups")

            # idx
            for n in tqdm(num_groups, desc="groups"):
                results_row = generate_benchmark_results(
                    spark_dataframe=sampled_sdf,
                    graph_structure=graph_structure,
                    ingest_function=ingest_functions.get(graph_structure).get(
                        load_strategy
                    ),
                    load_strategy=load_strategy,
                    num_groups=n,
                )
                results_df = append_results_to_dataframe(results_df, results_row)

                save_dataframe(results_df, ts)

                # clean up relationships
                delete_relationships(spark_session=spark_session)

            # idx + 1
            load_strategy = unsampled_tasks[idx + 1].get("load_strategy")
            num_groups = unsampled_tasks[idx + 1].get("num_groups")

            for n in tqdm(num_groups, desc="groups"):
                results_row = generate_benchmark_results(
                    spark_dataframe=sampled_sdf,
                    graph_structure=graph_structure,
                    ingest_function=ingest_functions.get(graph_structure).get(
                        load_strategy
                    ),
                    load_strategy=load_strategy,
                    num_groups=n,
                )
                results_df = append_results_to_dataframe(results_df, results_row)

                save_dataframe(results_df, ts)

                # clean up relationships
                delete_relationships(spark_session=spark_session)

            # refresh database
            restore_database(neo4j_driver=neo4j_driver)

            # wait for database to restore
            healthcheck(neo4j_driver=neo4j_driver)

    neo4j_driver.close()
