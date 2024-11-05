from .pipeline import Pipeline
from python import Load, Extract, Transform
import pandas as pd  
import sqlalchemy as sql
import os 
import logging

class RawPipeline(Pipeline):
    csv_extract: Extract
    sql_load: Load

    def __init__(self, csv_extracter: Extract = None, sql_loader: Load = None):
        self.csv_extract = csv_extracter
        self.sql_load = sql_loader
    
    def run(self):
        # Runs through each etl process.
        try:
            logging.info("Start the raw data extraction")
            # Points to docker internal localhost. Must be changed if deployed to production
            connection = sql.create_engine("postgresql+psycopg2://postgres:example@host.docker.internal:5431/postgres")

            # Extract data from various data sources
            stations = self.csv_extract.extract("include/train_data/stations/stations-2022-01.csv")
            disruptions = self.csv_extract.extract("include/train_data/disruptions/disruptions-2022.csv")
            distances = self.csv_extract.extract("include/train_data/distances/tariff-distances-2022-01.csv")

            # Load into Postgresql database.
            config = {"if_exist": "replace"}
            self.sql_load.load_data(stations, "Stations", config)
            self.sql_load.load_data(disruptions, "Disruptions", config)
            self.sql_load.load_data(stations, "Distances", config)
            # stations.to_sql("Stations", con=connection, if_exists="replace")
            # disruptions.to_sql("Disruptions", con=connection, if_exists="replace")
            # distances.to_sql("Distances", con=connection, if_exists="replace")
            
        except Exception as e:
            logging.error("Error within raw data extraction")
            raise e
