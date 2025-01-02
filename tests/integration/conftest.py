from typing import Any, Generator

import pytest
from neo4j import GraphDatabase


@pytest.fixture(scope="module")
def neo4j_driver() -> Generator[Any, Any, Any]:
    uri = "neo4j://localhost:7687"
    auth = ("neo4j", "password")
    driver = GraphDatabase.driver(uri, auth=auth)
    yield driver
    driver.close()

