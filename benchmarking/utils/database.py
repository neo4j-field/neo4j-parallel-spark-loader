from time import perf_counter
from typing import Any, List, Optional

from neo4j import Driver
from pyspark.sql import DataFrame, SparkSession

from benchmarking.utils.neo4j_driver import create_neo4j_driver
from neo4j_parallel_spark_loader import (
    bipartite,
    ingest_spark_dataframe,
    monopartite,
    predefined_components,
)


def create_constraints(neo4j_driver: Driver) -> None:
    queries = [
        "create constraint node_a_bp_constraint if not exists for (n:BPNodeA) require n.id is node key;",
        "create constraint node_b_bp_constraint if not exists for (n:BPNodeB) require n.id is node key;",
        "create constraint node_a_mp_constraint if not exists for (n:MPNodeA) require n.id is node key;",
        "create constraint node_a_pc_constraint if not exists for (n:PCNodeA) require n.id is node key;",
    ]

    with neo4j_driver.session() as session:
        [session.run(q) for q in queries]


def load_bipartite_nodes(spark_dataframe: DataFrame) -> None:
    node_a = spark_dataframe.select("source").distinct()
    node_b = spark_dataframe.select("target").distinct()

    query_a = """MERGE (:BPNodeA {id: event.source})"""
    query_b = """MERGE (:BPNodeB {id: event.target})"""

    (
        node_a.write.format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("query", query_a)
        .save()
    )

    (
        node_b.write.format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("query", query_b)
        .save()
    )

def load_monopartite_nodes(spark_dataframe: DataFrame) -> None:
    node_a = (
        spark_dataframe.select("source")
        .withColumnRenamed("source", "id")
        .distinct()
        .union(
            spark_dataframe.select("target")
            .withColumnRenamed("target", "id")
            .distinct()
        )
        .distinct()
    )

    query_a = """MERGE (:MPNodeA {id: event.id})"""

    (
        node_a.write.format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("query", query_a)
        .save()
    )

def load_predefined_components_nodes(spark_dataframe: DataFrame) -> None:
    node_a = (
        spark_dataframe.select("source")
        .withColumnRenamed("source", "id")
        .distinct()
        .union(
            spark_dataframe.select("target")
            .withColumnRenamed("target", "id")
            .distinct()
        )
        .distinct()
    )

    query_a = """MERGE (:PCNodeA {id: event.id})"""

    (
        node_a.write.format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("query", query_a)
        .save()
    )


def load_bipartite_relationships_in_serial(
    spark_dataframe: DataFrame, num_groups: Optional[int] = None
) -> List[float]:
    query = """
            MATCH (source:BPNodeA {id: event.source})
            MATCH (target:BPNodeB {id: event.target})
            CREATE (source)-[:HAS_RELATIONSHIP]->(target)
            """
    start = perf_counter()
    rels = spark_dataframe.select("source", "target")
    (
        rels.coalesce(1)
        .write.format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("query", query)
        .save()
    )
    return [0.0, perf_counter() - start]


def load_monopartite_relationships_in_serial(
    spark_dataframe: DataFrame, num_groups: Optional[int] = None
) -> List[float]:
    query = """
            MATCH (source:MPNodeA {id: event.source})
            MATCH (target:MPNodeA {id: event.target})
            CREATE (source)-[:HAS_RELATIONSHIP]->(target)
            """
    start = perf_counter()
    rels = spark_dataframe.select("source", "target")
    (
        rels.coalesce(1)
        .write.format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("query", query)
        .save()
    )
    return [0.0, perf_counter() - start]

def load_predefined_components_relationships_in_serial(
    spark_dataframe: DataFrame, num_groups: Optional[int] = None
) -> List[float]:
    query = """
            MATCH (source:PCNodeA {id: event.source})
            MATCH (target:PCNodeA {id: event.target})
            CREATE (source)-[:HAS_RELATIONSHIP]->(target)
            """
    start = perf_counter()
    rels = spark_dataframe.select("source", "target")
    (
        rels.coalesce(1)
        .write.format("org.neo4j.spark.DataSource")
        .mode("Append")
        .option("query", query)
        .save()
    )
    return [0.0, perf_counter() - start]


def load_bipartite_relationships_in_parallel(
    spark_dataframe: DataFrame, num_groups: int
) -> List[float]:
    query = """
    MATCH (source:BPNodeA {id: event.source})
    MATCH (target:BPNodeB {id: event.target})
    CREATE (source)-[:HAS_RELATIONSHIP]->(target)
    """
    start = perf_counter()
    grouped_and_batched_sdf = bipartite.group_and_batch_spark_dataframe(
        spark_dataframe=spark_dataframe,
        source_col="source",
        target_col="target",
        num_groups=num_groups,
    )
    proc_time = perf_counter() - start

    start = perf_counter()
    ingest_spark_dataframe(
        spark_dataframe=grouped_and_batched_sdf,
        save_mode="Append",
        options={"query": query},
        num_groups=num_groups,
    )
    return [proc_time, perf_counter() - start]


def load_monopartite_relationships_in_parallel(
    spark_dataframe: DataFrame, num_groups: int
) -> List[float]:
    query = """
    MATCH (source:MPNodeA {id: event.source})
    MATCH (target:MPNodeA {id: event.target})
    CREATE (source)-[:HAS_RELATIONSHIP]->(target)
    """
    start = perf_counter()
    grouped_and_batched_sdf = monopartite.group_and_batch_spark_dataframe(
        spark_dataframe=spark_dataframe,
        source_col="source",
        target_col="target",
        num_groups=num_groups,
    )
    proc_time = perf_counter() - start

    start = perf_counter()
    ingest_spark_dataframe(
        spark_dataframe=grouped_and_batched_sdf,
        save_mode="Append",
        options={"query": query},
        num_groups=num_groups,
    )

    return [proc_time, perf_counter() - start]


def load_predefined_components_relationships_in_parallel(
    spark_dataframe: DataFrame, num_groups: int
) -> List[float]:
    query = """
    MATCH (source:PCNodeA {id: event.source})
    MATCH (target:PCNodeA {id: event.target})
    CREATE (source)-[:HAS_RELATIONSHIP]->(target)
    """
    start = perf_counter()
    grouped_and_batched_sdf = predefined_components.group_and_batch_spark_dataframe(
        spark_dataframe=spark_dataframe,
        partition_col="partition_col",
        num_groups=num_groups,
    )
    proc_time = perf_counter() - start

    start = perf_counter()
    ingest_spark_dataframe(
        spark_dataframe=grouped_and_batched_sdf,
        save_mode="Append",
        options={"query": query},
        num_groups=num_groups,
    )
    return [proc_time, perf_counter() - start]

def validate_relationship_count(neo4j_driver: Driver, expected_count: int) -> None:
    results, _, _ = neo4j_driver.execute_query("MATCH ()-[:HAS_RELATIONSHIP]->() RETURN count(*) AS relCount")
    rel_count = results[0]['relCount']
    print(f"Loaded {rel_count} relationships.")
    assert rel_count == expected_count

def delete_relationships(neo4j_driver: Driver) -> None:
    query = """
MATCH ()-[r]->()
CALL (r) { DELETE r } IN TRANSACTIONS
"""
    with neo4j_driver.session() as session:
        session.run(query)


def restore_database(neo4j_driver: Driver) -> None:
    script = "create or replace database neo4j;"

    with neo4j_driver.session() as session:
        session.run(script)

def restore_aura_database(neo4j_driver: Driver) -> None:
    node_delete_query = """
MATCH (n)
CALL (n) {
DELETE n
} IN CONCURRENT TRANSACTIONS
    """
    with neo4j_driver.session() as session:
        session.run(node_delete_query)
    records, _, _ = neo4j_driver.execute_query("SHOW CONSTRAINTS YIELD name")
    for record in records:
        neo4j_driver.execute_query(f"DROP CONSTRAINT {record['name']}")
    
