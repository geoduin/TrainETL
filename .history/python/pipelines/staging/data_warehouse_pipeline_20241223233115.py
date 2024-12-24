from ..pipeline import Pipeline
from python import Load, Extract, Transform
from pandas import DataFrame

class DataWarehousePipeline(Pipeline):
    sql_extracter: Extract
    sql_load: Load
    transformers: Transform

    # Dim tables
    dim_date: DataFrame
    dim_cause: DataFrame
    dim_line_station: DataFrame
    dim_stations: DataFrame
    # Fact tables
    fact_disruption: DataFrame

    def __init__(self, sql_extracter: Extract, sql_load: Load, transformers: Transform):
        self.sql_extracter = sql_extracter
        self.sql_load = sql_load
        self.transformers = transformers

    def run(self):
        try:
            self._extract()
            self._load()
            self._transform()
        except Exception as e:
            print(e)
            raise e
    
    def _load(self):
        """
        Load data from the staging database
        """
        self.dim_date = self.sql_load.load_data('SELECT * FROM "Dim_DateTime";')
        self.dim_cause = self.sql_load.load_data('SELECT * FROM "Dim_Cause";')
        self.dim_line_station = self.sql_load.load_data('SELECT * FROM "Dim_Station_Lines";')
        self.dim_stations = self.sql_load.load_data('SELECT * FROM "Dim_Station";')
        self.fact_disruption = self.sql_load.load_data('SELECT * FROM "Disruptions";')
        print("Data loaded")
    
    def _extract(self):
        return self.sql_extracter.run()
    
    def _transform(self):
        # Transform cause
        # Transform disruption
        # Transform line_station
        # Transform station

        # Transform Fact
        return self.transformers.run()