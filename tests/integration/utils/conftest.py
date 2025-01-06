from typing import Dict

import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="function")
def bipartite_spark_dataframe(spark_fixture: SparkSession) -> DataFrame:
    # bipartite example data
    data = [
        {"source": "a", "target": "c", "prop": 0, "group": 0, "batch": 0},
        {"source": "a", "target": "d", "prop": 1, "group": 0, "batch": 1},
        {"source": "b", "target": "c", "prop": 2, "group": 1, "batch": 1},
        {"source": "b", "target": "d", "prop": 3, "group": 1, "batch": 0},
        {"source": "b", "target": "e", "prop": 4, "group": 0, "batch": 2},
    ]

    return spark_fixture.createDataFrame(data)


@pytest.fixture(scope="function")
def bipartite_ingest_options() -> Dict[str, str]:
    return {
        "schema.optimization.node.keys": "KEY",
        "relationship": "HAS_RELATIONSHIP",
        "relationship.save.strategy": "keys",
        "relationship.source.save.mode": "Overwrite",
        "relationship.source.labels": ":NodeA",
        "relationship.source.node.keys": "source:id",
        "relationship.target.save.mode": "Overwrite",
        "relationship.target.labels": ":NodeB",
        "relationship.target.node.keys": "target:id",
        "relationship.properties": "prop",
    }
