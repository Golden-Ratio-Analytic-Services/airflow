import datetime
import gzip
import logging
import requests
import shutil

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from utils import get_neo4j_connection

@dag(
    dag_id="process-addresses-dag",
    start_date=datetime.datetime(2023, 8, 20),
    schedule="@monthly",
)
def process_addresses_dag() -> None:
    """
    Dag for downloading and uploading address data to neo4j
    """

    @task
    def download_address_data(download_path: str) -> None:
        """
        Downloads addresses from OpenAddresses

        :param: download_path: The path to put the downloaded file
        :return: None
        """
        logging.info("Downloading address data...")
        address_url = 'https://v2.openaddresses.io/batch-prod/job/3065/source.geojson.gz'

        response = requests.get(address_url, stream=True)
        if response.status_code == 200:
            with open(download_path, 'wb+') as f:
                f.write(response.raw.read())

    @task
    def decompress_addresses(archive_path: str, target_path: str) -> None:
        """
        Decompresses the address file

        :param archive_path: The path of the archive to decompress
        :param target_path: The uncompressed file's path
        :return: None
        """
        logging.info("Decompressing address data...")
        with gzip.open(archive_path, 'rb') as archive_file:
            with open(target_path, 'wb+') as output_file:
                shutil.copyfileobj(archive_file, output_file)

    @task
    def create_indices() -> None:
        """
        Creates indices for faster lookup

        :return: None
        """
        logging.info("Creating indices for addresses")

        db_session = get_neo4j_connection()
        address_constraint_query = "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE"
        db_session.run(address_constraint_query)

    @task
    def load_addresses() -> None:
        """
        Loads addresses from OpenAddresses and attaches them to the
        nearest intersection.
        The data format is [longitude, latitude]

        :return: None
        """
        logging.info("Loading addresses into neo4j...")

        db_session = get_neo4j_connection()
        load_address_query = '''
        CALL apoc.load.json("file://addresses.geojson") YIELD value
        MERGE (a:Address {id: value.properties.hash})
        SET a.location = 
        point({latitude: value.geometry.coordinates[1], longitude: value.geometry.coordinates[0]}),
            a.full_address = value.properties.number + " " + value.properties.street + " " + value.properties.city + ", MX " + value.properties.postcode
        '''
        db_session.run(load_address_query)

    @task
    def load_address_intersection_relations() -> None:
        """
        Matches addresses to intersections

        :param driver: A driver to a neo4j database
        :return: None
        """
        logging.info("Loading address relations into neo4j...")

        db_session = get_neo4j_connection()
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

    download_path = '../data/addresses.geojson.gz'
    download_address_data(download_path) >> decompress_addresses(download_path, "../data/addresses.geojson")
    address_file_exists = FileSensor(
        task_id="wait_for_address_file",
        filepath="/opt/airflow/data/addresses.geojson",
        mode="poke",
        timeout=60 * 60 * 5,  # Timeout after 5 hours
        poke_interval=60,  # Check for the data every minute
    )
    address_file_exists >> create_indices() >> load_addresses() >> load_address_intersection_relations()


process_addresses_dag()
