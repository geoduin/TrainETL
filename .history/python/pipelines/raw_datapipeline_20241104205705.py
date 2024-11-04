from pipeline import Pipeline
from python.etl_classes.etl_abstract import AbstractETL
import pandas as pd  


class RawPipeline(Pipeline):
    etl_start_point: AbstractETL

    def __init__(self, etl_start: AbstractETL):
        self.etl_start_point = etl_start
    
    def run(self):
        # Runs through each etl process.
        self.etl_start_point.run()
