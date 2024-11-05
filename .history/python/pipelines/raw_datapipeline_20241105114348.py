from .pipeline import Pipeline
from python.etl_classes.etl_abstract import AbstractETL
from python import AbstractETL, Load, Extract, Transform
import pandas as pd  
import sqlalchemy as sql
import os 
import logging

class RawPipeline(Pipeline):
    csv_extract: AbstractETL
    sql_load: AbstractETL

    def __init__(self, csv_extracter: AbstractETL = None, sql_loader: AbstractETL = None):
        self.csv_extract = csv_extracter
        self.sql_load = sql_loader
    
    def run(self):
        # Runs through each etl process.
        try:
            logging.info("Start the raw data extraction")
            # Points to docker internal localhost. Must be changed if deployed to production
            connection = sql.create_engine("postgresql+psycopg2://postgres:example@host.docker.internal:5431/postgres")

            # Extract data from various data sources
            stations = self.csv_extract.run() pd.read_csv("include/train_data/stations/stations-2022-01.csv")
            disruptions = pd.read_csv("include/train_data/disruptions/disruptions-2022.csv")
            distances = pd.read_csv("include/train_data/distances/tariff-distances-2022-01.csv")

            # Load into Postgresql database.
            stations.to_sql("Stations", con=connection, if_exists="replace")
            disruptions.to_sql("Disruptions", con=connection, if_exists="replace")
            distances.to_sql("Distances", con=connection, if_exists="replace")
        except Exception as e:
            logging.error("Error within raw data extraction")
            raise e
