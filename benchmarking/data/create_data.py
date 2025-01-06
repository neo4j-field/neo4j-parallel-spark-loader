from random import randint

import numpy as np
import pandas as pd


def power_law_numbers(n: int, min_value: int, max_value: int, alpha: float):
    # Generate power law distributed numbers between 0 and 1
    # alpha is the power law exponent (typically between 2 and 3)
    raw_numbers = np.random.power(alpha, n)

    # Scale to desired range
    scaled_numbers = (raw_numbers * (max_value - min_value) + min_value).astype(int)
    return scaled_numbers


def generate_unpartitioned_dataframe(num_rows: int, alpha: float) -> pd.DataFrame:
    source_values = power_law_numbers(
        n=num_rows, min_value=1, max_value=num_rows, alpha=alpha
    )
    target_values = power_law_numbers(
        n=num_rows, min_value=1, max_value=num_rows, alpha=alpha
    )
    return pd.DataFrame({"source": source_values, "target": target_values})


def generate_partitioned_dataframe(
    num_rows: int, alpha: float, partition_count: int
) -> pd.DataFrame:
    mean_partition_size = num_rows / partition_count
    min_partition_size = int(mean_partition_size * 0.8)
    max_partition_size = int(mean_partition_size * 1.2)
    partition_sizes = [
        randint(min_partition_size, max_partition_size)
        for i in range(0, partition_count - 1)
    ]
    partition_sizes.append(num_rows - sum(partition_sizes))
    partition_dfs = []
    for i, partition_size in enumerate(partition_sizes):
        partition_df = generate_unpartitioned_dataframe(
            num_rows=partition_size, alpha=alpha
        )
        partition_df["partition_col"] = i
        partition_df["source"] = partition_df["source"].map(lambda x: f"{i}-{x}")
        partition_df["target"] = partition_df["target"].map(lambda x: f"{i}-{x}")
        partition_dfs.append(partition_df)
    return pd.concat(partition_dfs)


if __name__ == "__main__":
    NUM_ROWS = 100_000
    ALPHA = 2.0
    PARTITION_COUNT = 20
    root = "benchmarking/data/"

    bipartite_df = generate_unpartitioned_dataframe(num_rows=NUM_ROWS, alpha=ALPHA)
    bipartite_df.to_csv(root + "bipartite_data.csv", index=False)

    monopartite_df = generate_unpartitioned_dataframe(num_rows=NUM_ROWS, alpha=ALPHA)
    monopartite_df.to_csv(root + "monopartite_data.csv", index=False)

    predefined_components_df = generate_partitioned_dataframe(
        num_rows=NUM_ROWS, alpha=ALPHA, partition_count=PARTITION_COUNT
    )
    predefined_components_df.to_csv(
        root + "predefined_components_data.csv", index=False
    )
