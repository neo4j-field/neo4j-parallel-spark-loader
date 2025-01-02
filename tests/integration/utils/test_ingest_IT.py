from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from neo4j_parallel_spark_loader.utils.ingest import ingest_spark_dataframe


def test_ingest_with_bipartite_spark_dataframe(
    bipartite_spark_dataframe: DataFrame,
    bipartite_ingest_options: Dict[str, str],
    spark_fixture: SparkSession,
) -> None:
    ingest_spark_dataframe(
        spark_dataframe=bipartite_spark_dataframe,
        save_mode="Overwrite",
        options=bipartite_ingest_options,
    )
    nodes_query = "match (n) return labels(n) as labels, n{.*} as props"

    nodes: DataFrame = (
        spark_fixture.read.format("org.neo4j.spark.DataSource")
        .option("query", nodes_query)
        .load()
    )

    nodes.show()

    assert nodes.count() == 5

    rels_query = "match ()-[r]->() return type(r) as type, r{.*} as props"

    rels: DataFrame = (
        spark_fixture.read.format("org.neo4j.spark.DataSource")
        .option("query", rels_query)
        .load()
    )

    assert rels.count() == 5


