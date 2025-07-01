from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="function")
def bipartite_spark_dataframe(spark_fixture: SparkSession) -> DataFrame:
    # bipartite example data
    data = [
        {"source": "a", "target": "c", "prop": 0, "group": "a0", "batch": 0},
        {"source": "a", "target": "d", "prop": 1, "group": "a0", "batch": 1},
        {"source": "b", "target": "c", "prop": 2, "group": "b1", "batch": 1},
        {"source": "b", "target": "d", "prop": 3, "group": "b1", "batch": 0},
        {"source": "b", "target": "e", "prop": 4, "group": "a0", "batch": 2},
    ]

    return spark_fixture.createDataFrame(data)


@pytest.fixture(scope="function")
def node_spark_dataframe(spark_fixture: SparkSession) -> DataFrame:
    data = [
        {"id": "a"},
        {"id": "b"},
        {"id": "c"},
        {"id": "d"},
        {"id": "e"},
    ]

    return spark_fixture.createDataFrame(data)


@pytest.fixture(scope="function")
def relationship_spark_dataframe(spark_fixture: SparkSession) -> DataFrame:
    data = [
        {"source": "a", "target": "c", "prop": 0},
        {"source": "a", "target": "d", "prop": 1},
        {"source": "b", "target": "c", "prop": 2},
        {"source": "b", "target": "d", "prop": 3},
        {"source": "b", "target": "e", "prop": 4},
    ]

    return spark_fixture.createDataFrame(data)


@pytest.fixture(scope="function")
def bipartite_ingest_options() -> Dict[str, str]:
    return {
        "schema.optimization.node.keys": "KEY",
        "relationship": "HAS_BIPARTITE_RELATIONSHIP",
        "relationship.save.strategy": "keys",
        "relationship.source.save.mode": "Overwrite",
        "relationship.source.labels": ":NodeA",
        "relationship.source.node.keys": "source:id",
        "relationship.target.save.mode": "Overwrite",
        "relationship.target.labels": ":NodeB",
        "relationship.target.node.keys": "target:id",
        "relationship.properties": "prop",
    }
