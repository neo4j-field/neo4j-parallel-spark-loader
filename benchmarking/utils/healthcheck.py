import time

from neo4j import Driver
from neo4j.exceptions import DatabaseError


def healthcheck(neo4j_driver: Driver):
    """Confirm that Neo4j is running before continuing."""

    attempts = 0
    success = False
    # print("\nWaiting for Neo4j to Start...\n")
    time.sleep(3)
    while not success and attempts < 4:
        try:
            with neo4j_driver.session() as session:
                session.run("show databases")
            success = True
        except Exception:
            attempts += 1
            if attempts > 2:
                print(
                    f"Failed connection {attempts} | Waiting {(1 + attempts) * 2} seconds..."
                )
            time.sleep((1 + attempts) * 2)
    if not success:
        raise DatabaseError("HEALTHCHECK FAILED!")
