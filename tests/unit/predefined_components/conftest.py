from typing import Dict, List

import pytest


@pytest.fixture(scope="module")
def predefined_components_batching_data() -> List[Dict[str, int]]:
    return [
        {"source_group": 1, "target_group": 6, "partition_col": "a"},
        {"source_group": 1, "target_group": 7, "partition_col": "a"},
        {"source_group": 1, "target_group": 8, "partition_col": "a"},
        {"source_group": 0, "target_group": 9, "partition_col": "b"},
        {"source_group": 0, "target_group": 0, "partition_col": "b"},
    ]


@pytest.fixture(scope="module")
def predefined_components_grouping_data() -> List[Dict[str, int]]:
    return [
        {"source_node": 1, "target_node": 6, "partition_col": "a"},
        {"source_node": 1, "target_node": 7, "partition_col": "a"},
        {"source_node": 1, "target_node": 8, "partition_col": "a"},
        {"source_node": 0, "target_node": 9, "partition_col": "b"},
        {"source_node": 0, "target_node": 0, "partition_col": "b"},
    ]
