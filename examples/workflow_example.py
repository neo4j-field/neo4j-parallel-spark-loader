from pyspark.sql import DataFrame, SparkSession

from neo4j_parallel_spark_loader.bipartite import group_and_batch_spark_dataframe
from neo4j_parallel_spark_loader.utils import ingest_spark_dataframe

spark_session: SparkSession = (
    SparkSession.builder.appName("Workflow Example")
    .config(
        "spark.jars.packages",
        "org.neo4j:neo4j-connector-apache-spark_2.12:5.1.0_for_spark_3",
    )
    .config("neo4j.url", "neo4j://localhost:7687")
    .config("neo4j.authentication.type", "basic")
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "password")
    .getOrCreate()
)

purchase_df: DataFrame = spark_session.createDataFrame(data=...)

# Create batches and groups
batched_purchase_df = group_and_batch_spark_dataframe(
    purchase_df, "customer_id", "store_id", 8
)

# Load to Neo4j
includes_product_query = """
MATCH (o:Order {id: event['order_id']}),
(p:Product {id: event['product_id']})
MERGE (o)-[r:INCLUDES_PRODUCT]->(p)
ON CREATE SET r.quantity = event['quantity']
"""

# Load groups in parallel for each batch
ingest_spark_dataframe(purchase_df, "Overwrite", {"query": includes_product_query})
