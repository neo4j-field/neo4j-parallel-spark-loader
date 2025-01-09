from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list, countDistinct, max

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
    assert set([v for _, v in result.items()]) == {0, 1, 2}


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
        monopartite_batching_data, ["group"]
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
    source_or_target = sources.unionAll(targets)

    print(source_or_target.show())

    # Within a batch, a single source_or_target_value should be associated with at most 1 group.
    max_batch_st_group_count = (source_or_target.groupBy("source_or_target", "batch")
                                .agg(countDistinct("group").alias("distinct_count"))
                                .agg(max("distinct_count").alias("max_distinct_count"))
                                .collect())[0]["max_distinct_count"]
    
    assert max_batch_st_group_count == 1

