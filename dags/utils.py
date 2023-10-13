import logging
import neo4j


def get_neo4j_connection(address: str="neo4j") -> neo4j.Session:
    """
    Gets a session to neo4j
    """
    driver = neo4j.GraphDatabase.driver(
        f"bolt:{address}//:7687", auth=("neo4j", "neo4j222")
    )
    try:
        driver.verify_connectivity()
        session = driver.session(database="neo4j")
        return session
    except Exception as connection_error:
        logging.error("Failed to establish session to neo4j", connection_error)
        session.close()
        driver.close()