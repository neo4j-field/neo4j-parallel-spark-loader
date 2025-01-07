from typing import Dict, List

import pytest


@pytest.fixture(scope="module")
def monopartite_batching_data() -> List[Dict[str, int]]:
    return [
        {"source_group": 1, "target_group": 3},
        {"source_group": 2, "target_group": 4},
        {"source_group": 3, "target_group": 5},
        {"source_group": 4, "target_group": 6},
        {"source_group": 0, "target_group": 0},
    ]


@pytest.fixture(scope="module")
def monopartite_grouping_data() -> List[Dict[str, int]]:
    return [
        {"source_node": 1, "target_node": 3},
        {"source_node": 4, "target_node": 2},
        {"source_node": 3, "target_node": 5},
        {"source_node": 4, "target_node": 6},
        {"source_node": 0, "target_node": 0},
    ]
