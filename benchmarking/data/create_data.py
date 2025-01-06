import os
from random import randint
from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame

from benchmarking.utils.spark import create_spark_session

BIPARTITE_COLUMNS = ["source", "target", "relationship_type", "relationship_prop"]
MONOPARTITE_COLUMNS = ["source", "target", "relationship_type", "relationship_prop"]
PREDEFINED_COMPONENTS_COLUMNS = [
    "source",
    "target",
    "relationship_type",
    "relationship_prop",
    "partition_col",
]

RELATIONSHIP_TYPES = ["REL_A", "REL_B", "REL_C"]
RELATIONSHIP_PROPS = ["prop_a", "prop_b", "prop_c", "prop_d", "prop_e"]

spark_session = create_spark_session()


def _generate_spark_dataframe(
    source_values: List[Union[str, int]],
    target_values: List[Union[str, int]],
    columns: List[str],
    num_rows: int,
    partition_values: Optional[List[Union[str, int]]] = None,
) -> DataFrame:
    """Generate a Spark DataFrame based on the provided input."""

    if "partition_col" in columns:  # predefined comps
        raw_data = [
            _create_row_type_2(
                source_values=source_values,
                target_values=target_values,
                partition_values=partition_values,
            )
            for _ in range(num_rows)
        ]
    else:
        raw_data = [
            _create_row_type_1(source_values=source_values, target_values=target_values)
            for _ in range(num_rows)
        ]

    return spark_session.createDataFrame(raw_data)


def _pick_value_from_list(values: List[str]) -> Union[str, int]:
    return values[randint(0, len(values) - 1)]


def _create_row_type_1(
    source_values: List[str], target_values: List[str]
) -> Dict[str, Union[str, int]]:
    """Generate a row for bipartite or monopartite data."""

    return {
        "source": _pick_value_from_list(source_values),
        "target": _pick_value_from_list(target_values),
        "relationship_type": _pick_value_from_list(RELATIONSHIP_TYPES),
        "relationship_prop": _pick_value_from_list(RELATIONSHIP_PROPS),
    }


def _create_row_type_2(
    source_values: List[str],
    target_values: List[str],
    partition_values: Union[str, int],
) -> Dict[str, Union[str, int]]:
    """Generate a row for predefined components data."""

    return {
        "source": _pick_value_from_list(source_values),
        "target": _pick_value_from_list(target_values),
        "relationship_type": _pick_value_from_list(RELATIONSHIP_TYPES),
        "relationship_prop": _pick_value_from_list(RELATIONSHIP_PROPS),
        "partition_col": _pick_value_from_list(partition_values),
    }


def generate_bipartite_spark_dataframe(num_rows: int) -> DataFrame:
    source_values = ["a", "b", "c"]
    target_values = ["d", "e", "f"]
    return _generate_spark_dataframe(
        source_values=source_values,
        target_values=target_values,
        columns=BIPARTITE_COLUMNS,
        num_rows=num_rows,
    )


def generate_monopartite_spark_dataframe(num_rows: int) -> DataFrame:
    source_values = ["a", "b", "c"]
    target_values = ["b", "c", "d"]
    return _generate_spark_dataframe(
        source_values=source_values,
        target_values=target_values,
        columns=MONOPARTITE_COLUMNS,
        num_rows=num_rows,
    )


def generate_predefined_components_spark_dataframe(num_rows: int) -> DataFrame:
    source_values = ["a", "b", "c"]
    target_values = ["d", "e", "f"]
    partition_values = []
    return _generate_spark_dataframe(
        source_values=source_values,
        target_values=target_values,
        partition_values=partition_values,
        columns=PREDEFINED_COMPONENTS_COLUMNS,
        num_rows=num_rows,
    )


def write_spark_dataframe_to_csv(spark_dataframe: DataFrame, file_path: str) -> None:
    (
        spark_dataframe.coalesce(1)
        .write.option("header", "true")
        .option("sep", ",")
        .mode("overwrite")
        #   .format("csv")
        .csv(path=file_path)
    )


def clean_data_directory(directory: str = "benchmarking/data/") -> None:
    """Clean extra files created during csv generation.

    Delete files in the specified directory that don't end with '.csv' or '.py'

    Parameters:
    directory (str): Path to the directory to clean

    Returns:
    tuple: (list of deleted files, list of kept files)
    """

    # Ensure directory exists
    if not os.path.exists(directory):
        raise ValueError(f"Directory does not exist: {directory}")

    # Lists to track files
    deleted_files = []
    kept_files = []

    # Get all files in directory
    try:
        # Walk through directory and all subdirectories
        for root, _, files in os.walk(directory):
            for filename in files:
                file_path = os.path.join(root, filename)

                # Check if file ends with .csv or .py (case insensitive)
                if filename.lower().endswith((".csv", ".py")):
                    kept_files.append(file_path)
                else:
                    try:
                        os.remove(file_path)
                        deleted_files.append(file_path)
                    except OSError as e:
                        print(f"Error deleting {file_path}: {e}")

        # Print summary
        print("\nOperation Summary:")
        print(f"Deleted {len(deleted_files)} files:")
        for file in deleted_files:
            print(f"- {file}")

        print(f"\nKept {len(kept_files)} files:")
        for file in kept_files:
            print(f"- {file}")

        return deleted_files, kept_files

    except Exception as e:
        print(f"An error occurred: {e}")
        return [], []


if __name__ == "__main__":
    NUM_ROWS = 100_000
    root = "benchmarking/data/"
    write_spark_dataframe_to_csv(
        generate_bipartite_spark_dataframe(num_rows=NUM_ROWS),
        file_path=root + "bipartite/",
    )
    write_spark_dataframe_to_csv(
        generate_monopartite_spark_dataframe(num_rows=NUM_ROWS),
        file_path=root + "monopartite/",
    )
    # write_spark_dataframe_to_csv(generate_predefined_components_spark_dataframe(num_rows=NUM_ROWS), file_path=root+"predefined_components/")
    clean_data_directory(root)
