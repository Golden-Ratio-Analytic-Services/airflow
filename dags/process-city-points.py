import datetime
import requests
import pandas as pd

from airflow import DAG

with DAG(
   dag_id="process-city-points",
    start_date=datetime.datetime(2023, 8, 20),
    schedule="@monthly",
):
    remote_data_filename = "file-{{ts_nodash}}"
    cleaned_data_filename="mexico-city-lat-long.csv"

    def extract():
        """
        Extracts and saves geographic data about cities in Mexico as a CSV
        """
        city_data = requests.get("https://simplemaps.com/static/data/country-cities/mx/mx.csv")
        with open(remote_data_filename) as f:
            f.write(city_data.content)
    
    def transform():
        """
        Transforms the data from the data source into 
        """
        minimum_data = pd.read_csv(remote_data_filename)[['city','lat', 'lng']]
        minimum_data.to_csv(cleaned_data_filename)

    def load():
        raise NotImplementedError("Not completed yet")

    extract >> transform >> load