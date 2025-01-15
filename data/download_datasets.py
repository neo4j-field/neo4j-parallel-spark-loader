import json
import os
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests

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


def download_amazon_ratings_bipartite_spark_dataframe() -> None:
    K = "amazon_ratings"
    csv_loc = f"data/datasets/{K}.csv"

    if not os.path.exists(csv_loc):
        print("Gathering Amazon Dataset...")
        response = requests.get(FILE_ADDRESSES.get(K))
        zip_file = ZipFile(BytesIO(response.content))

        with zip_file.open(FILE_NAMES.get(K)) as file:
            content = file.read().decode("utf-8")
            df = pd.DataFrame(
                [c.split(",") for c in content.splitlines()],
                columns=["source", "target", "rating", "timestamp"],
            )
            df.to_csv(csv_loc, index=False, header=True)


def download_twitch_gamers_monopartite_spark_dataframe() -> None:
    K = "twitch_gamers"
    csv_loc = f"data/datasets/{K}.csv"
    if not os.path.exists(csv_loc):
        print("Gathering Twitch Dataset...")
        response = requests.get(FILE_ADDRESSES.get(K))
        zip_file = ZipFile(BytesIO(response.content))

        with zip_file.open(FILE_NAMES.get(K)) as file:
            content = file.read().decode("utf-8")
        df = pd.DataFrame(
            [c.split(",") for c in content.splitlines()], columns=["source", "target"]
        )

        df.to_csv(csv_loc, index=False, header=True)


def download_reddit_threads_predefined_components_spark_dataframe() -> None:
    K = "reddit_threads"
    csv_loc = f"data/datasets/{K}.csv"
    if not os.path.exists(csv_loc):
        print("Gathering Reddit Dataset...")
        response = requests.get(FILE_ADDRESSES.get(K))
        zip_file = ZipFile(BytesIO(response.content))

        with zip_file.open(FILE_NAMES.get(K)) as file:
            data = json.loads(file.read().decode("utf-8"))
            flattened = [
                [int(t[0]), int(sublist[0]), int(sublist[1])]
                for t in data.items()
                for sublist in t[1]
            ]
            df = pd.DataFrame(flattened, columns=["partition_col", "source", "target"])

            df.to_csv(csv_loc, index=False, header=True)
