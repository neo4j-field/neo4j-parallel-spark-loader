from typing import Any, Generator

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import DatabaseError


def create_neo4j_driver() -> Driver:
    uri = "neo4j://localhost:7687"
    auth = ("neo4j", "password")
    driver = GraphDatabase.driver(uri, auth=auth)
    return driver
