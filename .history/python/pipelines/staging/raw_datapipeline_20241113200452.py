from ..pipeline import Pipeline
from python import Load, Extract, Transform
import logging
from python import SQLHandler
from airflow import AirflowException

class RawPipeline(Pipeline):
    csv_extract: Extract
    sql_load: Load
    sql_handler: SQLHandler

    def __init__(self, csv_extracter: Extract = None, sql_loader: Load = None, sql_handler: SQLHandler = None):
        self.csv_extract = csv_extracter
        self.sql_load = sql_loader
        self.sql_handler = sql_handler
    
    def run(self):
        # Runs through each etl process.
        try:
            logging.info("Start the raw data extraction")
            # Extract data from various data sources
            stations = self.csv_extract.extract("include/train_data/stations/stations-2022-01.csv")
            disruptions = self.csv_extract.extract("include/train_data/disruptions/disruptions-2022.csv")
            distances = self.csv_extract.extract("include/train_data/distances/tariff-distances-2022-01.csv")

            # Load into Postgresql database.
            config = {"if_exist": "replace"}
            self.sql_load.load_data(stations, "Stations", **config)
            self.sql_load.load_data(disruptions, "Disruptions", **config)
            self.sql_load.load_data(distances, "Distances", **config)
        except Exception as e:
            logging.error("Error within raw data extraction")
            raise e
    
    def create_datedimension_tables(self):
        dim_datetime_query = """
            DROP TABLE IF EXISTS Dim_DateTime;
            CREATE TABLE Dim_DateTime (
                id BIGINT PRIMARY KEY,
                year SMALLINT,
                month SMALLINT,
                day SMALLINT,
                hour SMALLINT,
                minute SMALLINT
            );

            INSERT INTO Dim_DateTime (id, year, month, day, hour, minute)
                SELECT 
                    TO_CHAR(d, 'YYYYMMDDHH24MI')::BIGINT AS id,
                    EXTRACT(YEAR FROM d)::SMALLINT AS year,
                    EXTRACT(MONTH FROM d)::SMALLINT AS month,
                    EXTRACT(DAY FROM d)::SMALLINT AS day,
                    EXTRACT(HOUR FROM d)::SMALLINT AS hour,
                    EXTRACT(MINUTE FROM d)::SMALLINT AS minute
                FROM generate_series('2023-01-01 00:00:00'::TIMESTAMP, '2025-12-31 23:59:00'::TIMESTAMP, '1 minute') AS d;
            """
        
        succeeded = self.sql_handler.run_raw_query(dim_datetime_query)

        if(not succeeded):
            raise AirflowException("Creation tables have failed")