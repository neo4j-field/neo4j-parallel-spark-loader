from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from neo4j_parallel_spark_loader.utils.build_relationship import build_relationship


def test_ingest_with_bipartite_spark_dataframe(
    node_spark_dataframe: DataFrame,
    relationship_spark_dataframe: DataFrame,
    spark_fixture: SparkSession,
    #healthcheck: None,
) -> None:
    # Create the nodes first
    nodes_df = (
        node_spark_dataframe.write.format("org.neo4j.spark.DataSource")
        .mode("Overwrite")
        .option("labels", "NodeA")
        .option("node.keys", "id")
        .save()
    )
    nodes_query = "match (n:NodeA) return labels(n) as labels, n{.*} as props"

    nodes: DataFrame = (
        spark_fixture.read.format("org.neo4j.spark.DataSource")
        .option("query", nodes_query)
        .load()
    )

    nodes.show()

    assert nodes.count() == 5

    # Build Relationships
    build_relationship(relationship_spark_dataframe,
                       "HAS_RELATIONSHIP","NodeA","source:id","NodeA","target:id",
                       group_keys=["target"],
                       num_groups=2,
                       max_serial=4
                       )
    
    rels_query = "match (:NodeA)-[r]->() return type(r) as type, r{.*} as props"

    rels: DataFrame = (
        spark_fixture.read.format("org.neo4j.spark.DataSource")
        .option("query", rels_query)
        .load()
    )

    assert rels.count() == 5
