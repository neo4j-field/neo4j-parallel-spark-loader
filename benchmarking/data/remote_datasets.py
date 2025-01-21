import json
import os
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

SCHEMAS = {
    "amazon_ratings": StructType(
        [
            StructField("source", IntegerType(), True),
            StructField("target", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("timestamp", IntegerType(), True),
        ]
    ),
    "twitch_gamers": StructType(
        [
            StructField("source", IntegerType(), True),
            StructField("target", IntegerType(), True),
        ]
    ),
    "reddit_threads": StructType(
        [
            StructField("partition_col", IntegerType(), True),
            StructField("source", StringType(), True),
            StructField("target", StringType(), True),
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

    print("Gathering Amazon Dataset...")
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
        rating_df = spark_session.read.csv(rdd, schema=SCHEMAS.get("amazon_ratings"), header=False)

    return rating_df


def get_twitch_gamers_monopartite_spark_dataframe(
    spark_session: SparkSession,
) -> DataFrame:
    K = "twitch_gamers"
    print("Gathering Twitch Dataset...")
    # Download the ZIP file
    response = requests.get(FILE_ADDRESSES.get(K))
    zip_file = ZipFile(BytesIO(response.content))

    # Read the CSV file directly from the ZIP
    with zip_file.open(FILE_NAMES.get(K)) as file:
        # Convert to string buffer for Spark to read
        content = file.read().decode("utf-8")
        
        rdd = spark_session.sparkContext.parallelize(content.splitlines())

        # Convert RDD to DataFrame with schema
        twitch_df = spark_session.read.csv(rdd, schema=SCHEMAS.get("twitch_gamers"), header=False)

    return twitch_df



def get_reddit_threads_predefined_components_spark_dataframe(
    spark_session: SparkSession,
) -> DataFrame:
    K = "reddit_threads"
    print("Gathering Reddit Dataset...")
    response = requests.get(FILE_ADDRESSES.get(K))
    zip_file = ZipFile(BytesIO(response.content))

    with zip_file.open(FILE_NAMES.get(K)) as file:
        data = json.loads(file.read().decode("utf-8"))
        flattened = [
            [
                int(t[0]),
                str(sublist[0]) + "-" + str(t[0]),
                str(sublist[1]) + "-" + str(t[0]),
            ]
            for t in data.items()
            for sublist in t[1]
        ]
        
    reddit_df = spark_session.createDataFrame(flattened, schema=SCHEMAS.get("reddit_threads"))

    return reddit_df