from parallel_spark_loader.monopartite.batching import color_complete_graph_with_self_loops, create_ingest_batches_from_groups

def test_color_complete_graph_with_self_loops_with_even_vertices() -> None:
    result = color_complete_graph_with_self_loops(2)

    assert (0, 0) in result.keys()
    assert (1, 1) in result.keys()
    assert (0, 1) in result.keys()
    assert result.get((0,0)) == 0
    assert result.get((0,1)) == 1
    assert result.get((1,1)) == 0

def test_color_complete_graph_with_self_loops_with_odd_vertices() -> None:
    result = color_complete_graph_with_self_loops(3)

    assert len(result) == 6
    assert max([v for _, v in result.items()]) == 2

def test_color_complete_graph_with_self_loops_with_ten_vertices() -> None:
    color_complete_graph_with_self_loops(10)

def test_create_ingest_batches_from_groups(mocker) -> None:
    mocker.patch("parallel_spark_loader.monopartite.batching.color_complete_graph_with_self_loops", return_value=...)
    
    # result = create_ingest_batches_from_groups()
