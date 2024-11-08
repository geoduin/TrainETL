from ..pipeline import Pipeline
from python import Load, Extract, Transform
from airflow import AirflowException
import logging 
import pandas as pd 
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
        logging.info("Hello world")
        try:
            config = {"if_exist": "replace"}
            
            disruptions = self.sql_extracter.extract('SELECT * FROM "Disruptions"')

            disruptions["start_time"] = pd.to_datetime(disruptions["start_time"])
            disruptions["end_time"] = pd.to_datetime(disruptions["end_time"])

            disruptions = self.transformers.run(disruptions)

            self.sql_load.load_data(disruptions, "disruption")
        except Exception as m:
            logging.critical(m)
            raise AirflowException("Failed pipeline")
    