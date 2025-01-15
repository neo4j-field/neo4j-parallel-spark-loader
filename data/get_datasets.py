import os

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType

from .download_datasets import (
    download_amazon_ratings_bipartite_spark_dataframe,
    download_reddit_threads_predefined_components_spark_dataframe,
    download_twitch_gamers_monopartite_spark_dataframe,
)

SCHEMAS = {
    "amazon_ratings": StructType(
        [
            StructField("source_id", IntegerType(), True),
            StructField("target_id", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("timestamp", IntegerType(), True),
        ]
    ),
    "twitch_gamers": None,
    "reddit_threads": StructType(
        [
            StructField("graph_id", IntegerType(), True),
            StructField("source_id", IntegerType(), True),
            StructField("target_id", IntegerType(), True),
        ]
    ),
}


def get_amazon_ratings_bipartite_spark_dataframe(
    spark_session: SparkSession, max_cols: int = 1_000_000
) -> None:
    K = "amazon_ratings"
    csv_loc = f"data/datasets/{K}.csv"

    if not os.path.exists(csv_loc):
        download_amazon_ratings_bipartite_spark_dataframe()

    return spark_session.read.csv(
        csv_loc, schema=SCHEMAS.get(K), header=True, maxColumns=max_cols
    )


def get_twitch_gamers_monopartite_spark_dataframe(
    spark_session: SparkSession, max_cols: int = 1_000_000
) -> None:
    K = "twitch_gamers"
    csv_loc = f"data/datasets/{K}.csv"
    if not os.path.exists(csv_loc):
        download_twitch_gamers_monopartite_spark_dataframe()

    return spark_session.read.csv(csv_loc, header=True, maxColumns=max_cols)


def get_reddit_threads_predefined_components_spark_dataframe(
    spark_session: SparkSession, max_cols: int = 1_000_000
) -> None:
    K = "reddit_threads"
    csv_loc = f"data/datasets/{K}.csv"
    if not os.path.exists(csv_loc):
        download_reddit_threads_predefined_components_spark_dataframe()

    return spark_session.read.csv(csv_loc, header=True, maxColumns=max_cols)
