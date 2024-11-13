from ..pipeline import Pipeline
from python import Load, Extract, Transform
from airflow import AirflowException
import logging 
import pandas as pd 
from sqlalchemy import Integer, DateTime, String, BigInteger

class CleaningPipeline(Pipeline):
    """
    This pipeline will ensure that all of the data will be cleaned
    """
    sql_extracter: Extract
    sql_load: Load
    transformers: Transform

    def __init__(self, sql_extracter, sql_loader, transformer):
        self.sql_extracter = sql_extracter
        self.sql_load = sql_loader
        self.transformers = transformer

    def run(self):
        try:
            disruption_schema = {
                "rdt_id": Integer, 
                "ns_lines": String(500), 
                "rdt_lines": String(500), 
                "rdt_lines_id":String(500),
                "rdt_station_names":  String(500),
                "rdt_station_codes": String(500),
                "cause_nl": String(50),
                "cause_en": String(50),
                "statistical_cause_nl":	String(80),
                "statistical_cause_en":	String(80),
                "cause_group": String(50),
                "start_time": DateTime,
                "end_time": DateTime,
                "duration_minutes": Integer                                          			
                }
            config = {"if_exist": "replace"}
            dim_dates = self.sql_extracter('SELECT * FROM "Dim_DateTime"')
            disruptions = self.sql_extracter.extract('SELECT * FROM "Disruptions"')
            stations = self.sql_extracter.extract('SELECT * FROM "Stations"')
            distances = self.sql_extracter.extract('SELECT * FROM "Distances"')

            disruptions["start_time"] = pd.to_datetime(disruptions["start_time"])
            disruptions["end_time"] = pd.to_datetime(disruptions["end_time"])

            # Filter out all missing values
            disruptions = self.transformers.run(disruptions)

            self.sql_load.load_data(dim_dates, "Dim_DateTime", **config)
            self.sql_load.load_data(stations, "Stations", **config)
            self.sql_load.load_data(distances, "Distances", **config)
            
            config["dtypes"] = disruption_schema
            self.sql_load.load_data(disruptions, "Disruptions", **config)
        except Exception as m:
            logging.critical(m)
            raise AirflowException("Failed pipeline")
    