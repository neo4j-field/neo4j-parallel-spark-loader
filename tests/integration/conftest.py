from typing import Any, Generator
from neo4j import GraphDatabase
import pytest

@pytest.fixture(scope="session")
def neo4j_driver() -> Generator[Any, Any, Any]:
    uri = "neo4j://localhost:7687"
    auth = ("neo4j", "password")
    driver = GraphDatabase.driver(uri, auth=auth)
    yield driver
    driver.close()