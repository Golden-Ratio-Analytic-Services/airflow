import datetime
import logging

import osmnx as ox
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from utils import get_neo4j_connection

@dag(
    dag_id="process-roadways-dag",
    start_date=datetime.datetime(2023, 8, 20),
    schedule="@monthly",
)
def process_roadways_dag():
    
    @task
    def download_map_data() -> None:
        """
        Download the highway geometry

        :return: None
        """
        logging.info("Downloading Mexico road networks...")
        mexico_graph = ox.graph_from_place({"country": "Mexico"})
        gdf_nodes, gdf_relationships = ox.graph_to_gdfs(mexico_graph)
        gdf_nodes.reset_index(inplace=True)
        gdf_relationships.reset_index(inplace=True)
        logging.info("Finished download. Saving to disk")
        gdf_relationships.to_csv("../data/mexico_relations.csv")
        gdf_nodes.to_csv("../data/mexico_nodes.csv")
        logging.info("Finished saving road networks to disk")

    @task
    def create_indices() -> None:
        """
        Creates indicies for faster lookup

        :param db_session: Session for a neo4j connection
        :return: None
        """
        logging.info("Creating road network indices")
        db_session = get_neo4j_connection()
        constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intersection) REQUIRE i.osmid IS UNIQUE"
        rel_index_query = (
            "CREATE INDEX IF NOT EXISTS FOR ()-[r:ROAD_SEGMENT]-() ON r.osmids"
        )
        point_index_query = (
            "CREATE POINT INDEX IF NOT EXISTS FOR (i:Intersection) ON i.location"
        )
        constraints = [constraint_query, rel_index_query, point_index_query]
        for constraint in constraints:
            db_session.run(constraint)

    @task
    def load_intersections() -> None:
        """
        Loads all intersections from an OpenStreetMaps data file.
        The data is in the format [latitude, longitude]

        :param db_session: Session for a neo4j connection
        :return:
        """
        logging.info("Loading intersection data into neo4j")
        db_session = get_neo4j_connection()
        load_intersection_query = """
        LOAD CSV WITH HEADERS FROM 'file:///mexico_nodes.csv' AS road
        CALL {
          WITH road
          CREATE(intersection: Intersection{location: point({latitude: toFloat(road.y), longitude: toFloat(road.x)}), osmid: road.osmid})
        } IN TRANSACTIONS OF 1000 ROWS;
        """
        db_session.run(load_intersection_query)

    @task
    def load_intersection_relations() -> None:
        """
        Load the roads into neo4j

        :param db_session: Session for a neo4j connection
        :return: None
        """
        logging.info("Loading road network connections into neo4j")
        db_session = get_neo4j_connection()
        load_intersection_relation_query = """
        LOAD CSV WITH HEADERS FROM 'file:///mexico_relations.csv' AS segment
        CALL {
          WITH segment
          MATCH(intersection_a:Intersection {osmid: segment.u})
          MATCH(intersection_b: Intersection {osmid: segment.v})
          MERGE (intersection_a)-[road_segment:ROAD_SEGMENT {osmid: segment.osmid}]->(intersection_b)
            SET road_segment.oneway = segment.oneway,
            road_segment.ref = segment.ref,
            road_segment.name = segment.name,
            road_segment.highway = segment.highway,
            road_segment.max_speed = segment.maxspeed,
            road_segment.length = toFloat(segment.length)
        } IN TRANSACTIONS OF 1000 ROWS;
        """
        db_session.run(load_intersection_relation_query)

    download_map_data()
    node_file_exists = FileSensor(
        task_id="wait_for_node_file",
        filepath="/opt/airflow/data/mexico_nodes.csv",
        mode="poke",
        timeout=60 * 60 * 5,  # Timeout after 5 hours
        poke_interval=60,  # Check for the data every minute
    )
    relations_file_exists = FileSensor(
        task_id="wait_for_network_files",
        filepath="/opt/airflow/data/mexico_relations.csv",
        mode="poke",
        timeout=60 * 60 * 5,  # Timeout after 5 hours
        poke_interval=60,  # Check for the data every minute
    )
    (
        [relations_file_exists, node_file_exists]
        >> create_indices()
        >> load_intersections()
        >> load_intersection_relations()
    )
    logging.info("Finished handling road network data")


process_roadways_dag()
