import os
from typing import Optional

import pandas as pd


def load_results_data(
    package_version: str = "v0.2.0", file_name: Optional[str] = None
) -> pd.DataFrame:
    assert package_version.startswith(
        "v"
    ), "`package_version` must be in format 'v.<int>.<int>.<int>'"

    path = f"output/{package_version}/"

    if file_name is None:
        files = sorted([f for f in os.listdir(path)], reverse=True)
        file_name = files[0]

    return pd.read_csv(path + file_name)
