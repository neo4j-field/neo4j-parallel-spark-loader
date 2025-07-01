import time
from typing import Any, Generator

import pytest
from neo4j import Driver, GraphDatabase
from neo4j.exceptions import DatabaseError


@pytest.fixture(scope="session")
def neo4j_driver() -> Generator[Any, Any, Any]:
    uri = "neo4j://localhost:7687"
    auth = ("neo4j", "password")
    driver = GraphDatabase.driver(uri, auth=auth)
    yield driver
    driver.close()


@pytest.fixture(scope="session")
def healthcheck(neo4j_driver: Driver):
    """Confirm that Neo4j is running before running IT."""

    attempts = 0
    success = False
    print("\nWaiting for Neo4j to Start...\n")
    time.sleep(3)
    while not success or attempts > 3:
        try:
            with neo4j_driver.session() as session:
                session.run("show databases")
            success = True
        except Exception:
            attempts += 1
            print(
                f"failed connection {attempts} | waiting {(1 + attempts) * 2} seconds..."
            )
            time.sleep((1 + attempts) * 2)
    if not success:
        raise DatabaseError()
    yield

@pytest.fixture(scope="function")
def clear_neo4j_database(neo4j_driver: Driver):
    """Clear the Neo4j database before running IT."""
    with neo4j_driver.session() as session:
        session.run("match (n) detach delete n")
