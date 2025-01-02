from neo4j import Neo4jDriver, Result


def test_connection(neo4j_driver: Neo4jDriver, healthcheck: None) -> None:
    with neo4j_driver.session() as session:
        result: Result = session.run("SHOW DATABASES")
        data = result.data()
    assert {"neo4j", "system"} == {d.get("name") for d in data}
