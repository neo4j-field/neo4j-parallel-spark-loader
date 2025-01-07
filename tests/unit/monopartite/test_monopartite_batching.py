from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list

from neo4j_parallel_spark_loader.monopartite.batching import (
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


def test_create_ingest_batches_from_groups_no_duplicate_group_rels(
    spark_fixture: SparkSession, monopartite_dupe_batching_data: List[Dict[str, int]]
) -> None:
    sdf = spark_fixture.createDataFrame(
        monopartite_dupe_batching_data 
    )

    result: DataFrame = create_ingest_batches_from_groups(spark_dataframe=sdf)

    sources = result.select("source_node", "batch", "group").withColumnRenamed(
        "source_node", "source_or_target"
    )
    targets = result.select("target_node", "batch", "group").withColumnRenamed(
        "target_node", "source_or_target"
    )
    source_or_target = sources.union(targets)

    group_counts = source_or_target.groupBy("group", "source_or_target").count().orderBy("source_or_target")

    group_lists: DataFrame = group_counts.select("source_or_target", "group").groupBy("source_or_target").agg(collect_list("group"))

    for row in group_lists.rdd.collect():
        rels = []
        for group in row["collect_list(group)"]:
            r = (group[0], group[-1])
            r_rev = (group[-1], group[0])

            if r not in rels and r_rev not in rels:
                rels.append(r)
            else:
                assert r not in rels and r_rev not in rels

    # group_counts.show()
