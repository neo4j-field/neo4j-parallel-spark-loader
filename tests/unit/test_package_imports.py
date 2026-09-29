def test_build_relationship_importable_from_package_root() -> None:
    from neo4j_parallel_spark_loader import build_relationship
    from neo4j_parallel_spark_loader.utils.build_relationship import (
        build_relationship as build_relationship_impl,
    )

    assert build_relationship is build_relationship_impl


def test_build_relationship_in_package_all() -> None:
    import neo4j_parallel_spark_loader

    assert "build_relationship" in neo4j_parallel_spark_loader.__all__
