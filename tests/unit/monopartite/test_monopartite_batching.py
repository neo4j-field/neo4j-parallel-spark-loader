from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from parallel_spark_loader.monopartite.batching import (
    color_complete_graph_with_self_loops,
    create_ingest_batches_from_groups,
)


def test_color_complete_graph_with_self_loops_with_even_vertices() -> None:
    result = color_complete_graph_with_self_loops(2)

    assert (0, 0) in result.keys()
    assert (1, 1) in result.keys()
    assert (0, 1) in result.keys()
    assert result.get((0, 0)) == 0
    assert result.get((0, 1)) == 1
    assert result.get((1, 1)) == 0


def test_color_complete_graph_with_self_loops_with_odd_vertices() -> None:
    result = color_complete_graph_with_self_loops(3)

    assert len(result) == 6
    assert max([v for _, v in result.items()]) == 2


def test_color_complete_graph_with_self_loops_with_ten_vertices() -> None:
    res = color_complete_graph_with_self_loops(10)

    node_color_map = dict()  # {node id: [color id, ...]}

    for edge, color in res.items():
        s, t = edge
        if color not in node_color_map.get(s, []):
            node_color_map[s] = node_color_map.get(s, []) + [color]
        else:
            raise AssertionError(f"node {s} | color {color}")

        if s != t and color not in node_color_map.get(t, []):
            node_color_map[t] = node_color_map.get(t, []) + [color]
        elif s == t:
            continue
        else:
            raise AssertionError(f"node {t} | color {color}")


def test_create_ingest_batches_from_groups(
    spark_fixture: SparkSession, monopartite_batching_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(
        monopartite_batching_data, ["source_group", "target_group"]
    )

    result = create_ingest_batches_from_groups(spark_dataframe=sdf)

    null_count = result.filter(col("batch").isNull()).count()
    assert null_count == 0
