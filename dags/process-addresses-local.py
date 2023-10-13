import logging

from utils import get_neo4j_connection


def create_indices() -> None:
    """
    Creates indices for faster lookup

    :return: None
    """
    logging.info("Creating indices for addresses")

    db_session = get_neo4j_connection('localhost')
    address_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE"
    db_session.run(address_constraint_query)


def load_addresses() -> None:
    """
    Loads addresses from OpenAddresses and attaches them to the
    nearest intersection.
    The data format is [longitude, latitude]

    :return: None
    """
    logging.info("Loading addresses into neo4j...")

    db_session = get_neo4j_connection('localhost')
    load_address_query = '''
    CALL apoc.load.json("file://addresses.geojson") YIELD value
    MERGE (a:Address {id: value.properties.hash})
    SET a.location = 
    point({latitude: value.geometry.coordinates[1], longitude: value.geometry.coordinates[0]}),
        a.full_address = value.properties.number + " " + value.properties.street + " " + value.properties.city + ", MX " + value.properties.postcode
    '''
    db_session.run(load_address_query)

def load_address_intersection_relations() -> None:
    """
    Matches addresses to intersections

    :param driver: A driver to a neo4j database
    :return: None
    """
    logging.info("Loading address relations into neo4j...")

    db_session = get_neo4j_connection('localhost')
    load_address_relations = '''
    MATCH (p:Address) WHERE NOT EXISTS ((p)-[:NEAREST_INTERSECTION]->(:Intersection))
    CALL {
        WITH p
      MATCH (i:Intersection)
      WHERE point.distance(i.location, p.location) < 2000
        WITH i
      ORDER BY point.distance(p.location, i.location) ASC 
      LIMIT 1
        RETURN i
    }
    WITH p, i
    MERGE (p)-[r:NEAREST_INTERSECTION]->(i)
    SET r.length = point.distance(p.location, i.location)
    '''
    db_session.run(load_address_relations)


create_indices()
load_addresses()
load_address_intersection_relations()
