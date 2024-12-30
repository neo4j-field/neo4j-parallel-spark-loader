from typing import Dict, List

import pytest


@pytest.fixture(scope="module")
def bipartite_batching_data() -> List[Dict[str, int]]:
    return [
        {"source_group": 1, "target_group": 0},
        {"source_group": 1, "target_group": 1},
        {"source_group": 1, "target_group": 2},
        {"source_group": 0, "target_group": 3},
        {"source_group": 0, "target_group": 1},
    ]


@pytest.fixture(scope="module")
def bipartite_grouping_data() -> List[Dict[str, int]]:
    return [
        {"source_node": 1, "target_node": 0},
        {"source_node": 1, "target_node": 1},
        {"source_node": 1, "target_node": 2},
        {"source_node": 0, "target_node": 3},
        {"source_node": 0, "target_node": 1},
    ]
