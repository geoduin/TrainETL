from .pipeline import Pipeline
from python.etl_classes.etl_abstract import AbstractETL
import pandas as pd  
import sqlalchemy as sql

import logging

class RawPipeline(Pipeline):
    etl_start_point: AbstractETL

    def __init__(self, etl_start: AbstractETL = None):
        self.etl_start_point = etl_start
    
    def run(self):
        # Runs through each etl process.
        try:
            logging.info("Start the raw data extraction")
            # Points to docker internal localhost. Must be changed if deployed to production
            connection = sql.create_engine("postgresql+psycopg2://postgres:example@host.docker.internal:5431/postgres")

            # Extract data from various data sources
            stations = pd.read_csv("include/train_data/stations/stations-2022-01.csv")
            disruptions = pd.read_csv("include/train_data/disruptions/disruptions-2022.csv")
            distances = pd.read_csv("include/train_data/distances/tariff-distances-2022-01.csv")

            # Load into Postgresql database.
            stations.to_sql("Stations", con=connection, if_exists="replace")
            disruptions.to_sql("Disruptions", con=connection, if_exists="replace")
            distances.to_sql("Distances", con=connection, if_exists="replace")
        except:
            raise
