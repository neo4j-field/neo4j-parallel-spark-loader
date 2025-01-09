from random import randint
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import DataFrame


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


@pytest.fixture(scope="function")
def input_spark_dataframe() -> DataFrame:
    data = generate_unpartitioned_dataframe(10_000, 2.0)

@pytest.fixture(scope="module")
def monopartite_batching_data() -> List[Dict[str, int]]:
    return [
        {"group": "1--3", "source_group": 1, "target_group": 3},
        {"group": "2--4", "source_group": 2, "target_group": 4},
        {"group": "3--5", "source_group": 5, "target_group": 3},
        {"group": "4--6", "source_group": 6, "target_group": 4},
        {"group": "0--0", "source_group": 0, "target_group": 0},
    ]