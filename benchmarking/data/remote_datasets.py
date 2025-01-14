import json
from io import BytesIO
from zipfile import ZipFile

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType

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

FILE_ADDRESSES = {
    "amazon_ratings": "https://nrvis.com/download/data/dynamic/rec-amazon-ratings.zip",
    "twitch_gamers": "https://snap.stanford.edu/data/twitch_gamers.zip",
    "reddit_threads": "https://snap.stanford.edu/data/reddit_threads.zip",
}

FILE_NAMES = {
    "amazon_ratings": "rec-amazon-ratings.edges",
    "twitch_gamers": "large_twitch_edges.csv",
    "reddit_threads": "reddit_threads/reddit_edges.json",
}


def get_amazon_ratings_bipartite_spark_dataframe(
    spark_session: SparkSession,
) -> DataFrame:
    K = "amazon_ratings"
    # Download the ZIP file
    response = requests.get(FILE_ADDRESSES.get(K))
    zip_file = ZipFile(BytesIO(response.content))

    # Read the CSV file directly from the ZIP
    with zip_file.open(FILE_NAMES.get(K)) as file:
        # Convert to string buffer for Spark to read
        content = file.read().decode("utf-8")

        # Create RDD from content
        rdd = spark_session.sparkContext.parallelize(content.splitlines())

        # Convert RDD to DataFrame with schema
        return spark_session.read.csv(rdd, schema=SCHEMAS.get(K), header=False)


def get_twitch_gamers_monopartite_spark_dataframe(
    spark_session: SparkSession,
) -> DataFrame:
    K = "twitch_gamers"
    # Download the ZIP file
    response = requests.get(FILE_ADDRESSES.get(K))
    zip_file = ZipFile(BytesIO(response.content))

    # Read the CSV file directly from the ZIP
    with zip_file.open(FILE_NAMES.get(K)) as file:
        # Convert to string buffer for Spark to read
        content = file.read().decode("utf-8")

        # Create RDD from content
        rdd = spark_session.sparkContext.parallelize(content.splitlines())

        # Convert RDD to DataFrame
        return spark_session.read.csv(rdd, header=True)


def get_reddit_threads_predefined_components_spark_dataframe(
    spark_session: SparkSession,
) -> DataFrame:
    K = "reddit_threads"
    # Download and read the zip file content
    response = requests.get(FILE_ADDRESSES.get(K))
    zip_file = ZipFile(BytesIO(response.content))

    # Read the JSON file content from the zip
    with zip_file.open(FILE_NAMES.get(K)) as file:
        # Parse JSON content
        data = json.loads(file.read().decode("utf-8"))
        flattened = [
            [int(t[0]), int(sublist[0]), int(sublist[1])]
            for t in data.items()
            for sublist in t[1]
        ]

        # Create DataFrame from parsed JSON
        return spark_session.createDataFrame(flattened, schema=SCHEMAS.get(K))
