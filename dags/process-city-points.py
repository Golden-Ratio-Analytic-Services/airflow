import datetime
import logging

import pandas as pd
import requests
from airflow.decorators import dag, task


@dag(
    dag_id="process-city-points",
    start_date=datetime.datetime(2023, 8, 20),
    schedule="@monthly",
)
def process_city_points_dag():
    remote_data_filename = "mexico-city-data.csv"
    cleaned_data_filename = "mexico-city-lat-long.csv"

    @task
    def extract():
        """
        Extracts and saves geographic data about cities in Mexico as a CSV
        """
        logging.info("Downloading city data...")
        city_data = requests.get(
            "https://simplemaps.com/static/data/country-cities/mx/mx.csv"
        )
        with open(remote_data_filename, "wb") as f:
            f.write(city_data.content)
        logging.info("Successfully downloaded data")

    @task
    def transform():
        """
        Transforms the data from the data source into
        """
        logging.info("Transforming city data...")
        minimum_data = pd.read_csv(remote_data_filename)[["city", "lat", "lng"]]
        minimum_data.to_csv(cleaned_data_filename)
        logging.info("Successfully transformed data")

    @task
    def load():
        logging.info("Loading city data into the database")
        raise NotImplementedError("Not completed yet")

    extract() >> transform() >> load()


process_city_points_dag()
