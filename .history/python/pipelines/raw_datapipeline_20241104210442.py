from pipeline import Pipeline
from python.etl_classes.etl_abstract import AbstractETL
import pandas as pd  


class RawPipeline(Pipeline):
    etl_start_point: AbstractETL

    def __init__(self, etl_start: AbstractETL = None):
        self.etl_start_point = etl_start
    
    def run(self):
        # Runs through each etl process.
        
        # Extract data from various data sources
        stations = pd.read_csv("include/stations/stations-2022-01.csv")
        disruptions = pd.read_csv("include/disruptions/disruptions-2022.csv")
        distances = pd.read_csv("include/distances/tariff-distances-2022-01.csv")
        
        # Load into Postgresql database.
        stations.to_sql("Stations", con=None, if_exists="replace")
        disruptions.to_sql("Disruptions", con=None, if_exists="replace")
        distances.to_sql("Distances", con=None, if_exists="replace")