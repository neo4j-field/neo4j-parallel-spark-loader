import argparse
import warnings
from datetime import datetime
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from tqdm import tqdm

from benchmarking.data.remote_datasets import (
    get_amazon_ratings_bipartite_spark_dataframe,
    get_reddit_threads_predefined_components_spark_dataframe,
    get_twitch_gamers_monopartite_spark_dataframe,
)
from benchmarking.utils.database import *
from benchmarking.utils.healthcheck import healthcheck
from benchmarking.utils.neo4j_driver import create_neo4j_driver
from benchmarking.utils.results import (
    append_results_to_dataframe,
    create_results_dataframe,
    generate_benchmark_results,
    get_package_version,
    save_dataframe,
)
from benchmarking.utils.spark import (
    create_spark_session,
    get_current_spark_num_workers,
    get_spark_details,
    load_data_into_spark_dataframe,
    sample_spark_dataframe,
)

warnings.simplefilter(action="ignore", category=FutureWarning)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env", help="The environment benchmarking is run in.")
    ap.add_argument("-d", "--datatype", help="Generated or real")

    args = ap.parse_args()

    spark_session: SparkSession = create_spark_session()
    neo4j_driver = create_neo4j_driver()

    if args.datatype and args.datatype == "generated":
        bp_sdf = load_data_into_spark_dataframe(spark_session, "bipartite")
        mp_sdf = load_data_into_spark_dataframe(spark_session, "monopartite")
        pc_sdf = load_data_into_spark_dataframe(spark_session, "predefined_components")
        DATASET_NAMES = {
            "bipartite": "generated",
            "monopartite": "generated",
            "predefined_components": "generated",
        }
    else:
        bp_sdf = get_amazon_ratings_bipartite_spark_dataframe(spark_session)
        mp_sdf = get_twitch_gamers_monopartite_spark_dataframe(spark_session)
        pc_sdf = get_reddit_threads_predefined_components_spark_dataframe(spark_session)
        DATASET_NAMES = {
            "bipartite": "amazon_ratings",
            "monopartite": "twitch_gamers",
            "predefined_components": "reddit_threads",
        }

    if args.env:
        match str(args.env).lower():
            case "databricks":
                ENV = "databricks"
            case "local":
                ENV = "local"
            case _:
                ENV = "unknown"
    else:
        ENV = "unknown"

    AURA = ENV == "databricks"

    static_cols: Dict[str, Any] = get_spark_details(spark_session=spark_session)
    static_cols.update({"neo4j_parallel_spark_loader_version": get_package_version()})
    static_cols.update({"environment": ENV})

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
    sample_sizes = [10, 100, 1_000, 10_000, 100_000, 1_000_000]

    sdfs = {0: bp_sdf, 2: mp_sdf, 4: pc_sdf}

    SERIAL_GROUPS = [1]
    BIPARTITE_GROUPS = [3, 5]
    MONOPARTITE_GROUPS = [3, 5]
    PREDEFINED_COMPONENTS_GROUPS = [3, 5]

    unsampled_tasks = [
        {
            "graph_structure": "bipartite",
            "load_strategy": "serial",
            "num_groups": SERIAL_GROUPS,
            "dataset_name": DATASET_NAMES.get("bipartite"),
        },
        {
            "graph_structure": "bipartite",
            "load_strategy": "parallel",
            "num_groups": BIPARTITE_GROUPS,
            "dataset_name": DATASET_NAMES.get("bipartite"),
        },
        {
            "graph_structure": "monopartite",
            "load_strategy": "serial",
            "num_groups": SERIAL_GROUPS,
            "dataset_name": DATASET_NAMES.get("monopartite"),
        },
        {
            "graph_structure": "monopartite",
            "load_strategy": "parallel",
            "num_groups": MONOPARTITE_GROUPS,
            "dataset_name": DATASET_NAMES.get("monopartite"),
        },
        {
            "graph_structure": "predefined_components",
            "load_strategy": "serial",
            "num_groups": SERIAL_GROUPS,
            "dataset_name": DATASET_NAMES.get("predefined_components"),
        },
        {
            "graph_structure": "predefined_components",
            "load_strategy": "parallel",
            "num_groups": PREDEFINED_COMPONENTS_GROUPS,
            "dataset_name": DATASET_NAMES.get("predefined_components"),
        },
    ]

    # init new dataframe
    results_df = create_results_dataframe()

    # mark benchmarking start time
    ts = str(datetime.now())

    # wait for database to be online before running
    healthcheck(neo4j_driver=neo4j_driver)

    for idx in tqdm(range(0, len(unsampled_tasks), 2), desc="graph structure"):
        # for idx in range(0, len(unsampled_tasks), 2):
        print(
            unsampled_tasks[idx].get("graph_structure"),
            " | ",
            unsampled_tasks[idx].get("dataset_name"),
        )
        dataset_name = unsampled_tasks[idx].get("dataset_name")

        for s in tqdm(sample_sizes, desc="sample sizes"):
            # for s in sample_sizes:
            sampled_sdf: DataFrame = sample_spark_dataframe(sdfs.get(idx), s)

            # SERIAL
            # ------
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
                # for n in num_groups:
                results_row = generate_benchmark_results(
                    spark_dataframe=sampled_sdf,
                    graph_structure=graph_structure,
                    ingest_function=ingest_functions.get(graph_structure).get(
                        load_strategy
                    ),
                    load_strategy=load_strategy,
                    num_groups=n,
                    static_columns=static_cols,
                    dataset_name=dataset_name,
                )

                results_df = append_results_to_dataframe(results_df, results_row)

                save_dataframe(results_df, ts)

                # clean up relationships
                # print("\nDELETING RELATIONSHIPS\n")
                delete_relationships(neo4j_driver=neo4j_driver)

            # PARALLEL
            # --------
            # idx + 1
            load_strategy = unsampled_tasks[idx + 1].get("load_strategy")
            num_groups = unsampled_tasks[idx + 1].get("num_groups")

            for n in tqdm(num_groups, desc="groups"):
                # for n in num_groups:
                # print(f"Loading {s} Rows | {n} Spark Nodes")
                results_row = generate_benchmark_results(
                    spark_dataframe=sampled_sdf,
                    graph_structure=graph_structure,
                    ingest_function=ingest_functions.get(graph_structure).get(
                        load_strategy
                    ),
                    load_strategy=load_strategy,
                    num_groups=n,
                    static_columns=static_cols,
                    dataset_name=dataset_name,
                )
                results_df = append_results_to_dataframe(results_df, results_row)

                save_dataframe(results_df, ts)

                # clean up relationships
                # print("\nDELETING RELATIONSHIPS\n")
                delete_relationships(neo4j_driver=neo4j_driver)

            # refresh database
            if AURA:
                # delete all nodes and rels, since can't drop databases yet...
                restore_aura_database(neo4j_driver=neo4j_driver)
            else:
                restore_database(neo4j_driver=neo4j_driver)

            # wait for database to restore
            healthcheck(neo4j_driver=neo4j_driver)

    neo4j_driver.close()
