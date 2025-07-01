import os
from typing import Any, Generator, Optional

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import DatabaseError


def create_neo4j_driver(
    neo4j_username: Optional[str] = None,
    neo4j_password: Optional[str] = None,
    neo4j_uri: Optional[str] = None,
    neo4j_database: Optional[str] = None,
) -> Driver:
    uri = neo4j_uri or os.environ.get("NEO4J_URI", "neo4j://localhost:7687")
    username = neo4j_username or os.environ.get("NEO4J_USERNAME", "neo4j")
    password = neo4j_password or os.environ.get("NEO4J_PASSWORD", "<PASSWORD>")
    auth = (username, password)
    driver = GraphDatabase.driver(uri, auth=auth)
    return driver
