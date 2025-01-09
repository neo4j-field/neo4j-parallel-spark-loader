from typing import Dict, List

import pytest


@pytest.fixture(scope="module")
def monopartite_batching_data() -> List[Dict[str, int]]:
    return [
        {"group": "1 -- 3", "source_group": 1, "target_group": 3},
        {"group": "2 -- 4", "source_group": 2, "target_group": 4},
        {"group": "3 -- 5", "source_group": 5, "target_group": 3},
        {"group": "4 -- 6", "source_group": 6, "target_group": 4},
        {"group": "0 -- 0", "source_group": 0, "target_group": 0},
    ]


@pytest.fixture(scope="module")
def monopartite_dupe_batching_data() -> List[Dict[str, int]]:
    return [
        {
            "source_group": 1,
            "target_group": 3,
            "group": "1 -- 3",
            "source_node": 1,
            "target_node": 3,
        },
        {
            "source_group": 2,
            "target_group": 4,
            "group": "2 -- 4",
            "source_node": 4,
            "target_node": 2,
        },
        {
            "source_group": 3,
            "target_group": 5,
            "group": "3 -- 5",
            "source_node": 3,
            "target_node": 5,
        },
        {
            "source_group": 4,
            "target_group": 6,
            "group": "4 -- 6",
            "source_node": 4,
            "target_node": 6,
        },
        {
            "source_group": 0,
            "target_group": 0,
            "group": "0 -- 0",
            "source_node": 0,
            "target_node": 0,
        },
        {
            "source_group": 3,
            "target_group": 1,
            "group": "1 -- 3",
            "source_node": 3,
            "target_node": 1,
        },
        {
            "source_group": 0,
            "target_group": 0,
            "group": "0 -- 0",
            "source_node": 0,
            "target_node": 0,
        },
        {
            "source_group": 6,
            "target_group": 4,
            "group": "4 -- 6",
            "source_node": 6,
            "target_node": 4,
        },
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
