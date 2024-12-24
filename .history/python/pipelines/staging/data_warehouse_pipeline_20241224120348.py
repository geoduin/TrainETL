from ..pipeline import Pipeline
from python import Load, Extract, Transform, SnowflakeHandler
from pandas import DataFrame
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine

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

    def __init__(self, sql_extracter: Extract, sql_load: Load, transformers: Transform, snowflake_handler: SnowflakeHandler):
        self.sql_extracter = sql_extracter
        self.sql_load = sql_load
        self.transformers = transformers
        self.snowflake_handler = snowflake_handler

    def run(self):
        """
        This will run as an ELT pipeline.
        """
        try:
            self._extract()
            self._load()
            self._transform()
        except Exception as e:
            print(e)
            raise e
    
    def create_temp_tables(self):
        print("Create temporal tables")
        local_engine = self.snowflake_handler.engine
        local_connection = self.snowflake_handler.get_connection()
        METADATA = MetaData(bind=local_connection)
        temp_table_date = Table("Temp_DateTime", 
                                METADATA, 
                                Column("id", Integer, primary_key=True), 
                                Column("year", String),
                                Column("month", String),
                                Column("day", String),
                                Column("hour", String),
                                Column("minute", String))
        temp_table_cause = Table("Temp_Cause", 
                                METADATA, 
                                Column("cause_id", Integer, primary_key=True), 
                                Column("cause", String),
                                Column("cause_group", String),
                                Column("statistical_cause", String))
        temp_table_station = Table("Temp_Stations", 
                                METADATA, 
                                Column("id", Integer, primary_key=True), 
                                Column("code", String),
                                Column("uic", Integer),
                                Column("name_short", String),
                                Column("name_medium", String),
                                Column("name_long", String),
                                Column("slug", String),
                                Column("country", String),
                                Column("type", String),
                                Column("geo_lat", String),
                                Column("geo_lng", String))
        
        temp_table_lines = Table("Temp_Station_Lines", 
                                METADATA, 
                                Column("rdt_id", Integer, primary_key=True), 
                                Column("rdt_lines_id", Integer),
                                Column("rdt_lines", Integer),
                                Column("begin_station", String),
                                Column("end_station", String))
        
        temp_table_disruptions = Table("Temp_Disruptions", 
                                METADATA, 
                                Column("id", Integer, primary_key=True), 
                                Column("start_time", Integer),
                                Column("end_time", Integer),
                                Column("duration_minutes", String),
                                Column("cause_id", String))
        METADATA.create_all(local_engine)
    
    def _load(self):
        """
        Load data from the staging database
        """
        config = {"if_exist": "replace", "chunksize": 16000}
        config_other = {"if_exist": "append"}
        # self.sql_load.load_data(self.dim_date, "Temp_DateTime", **config)
        self.sql_load.load_data(self.dim_cause, "Temp_Cause", **config_other)
        self.sql_load.load_data(self.dim_stations, "Temp_Stations", **config_other)
        self.sql_load.load_data(self.fact_disruption, "Temp_Disruptions", **config_other)
        self.sql_load.load_data(self.dim_line_station, "Temp_Station_Lines", **config_other)
        print("Data loaded")
    
    def _extract(self):
        """
        Extract data from the staging database
        """
        # self.dim_date = self.sql_extracter.extract('SELECT * FROM "Dim_DateTime";')
        self.dim_cause = self.sql_extracter.extract('SELECT * FROM "Dim_Cause";')
        self.dim_line_station = self.sql_extracter.extract('SELECT * FROM "Dim_Station_Lines";')
        self.dim_stations = self.sql_extracter.extract('SELECT * FROM "Dim_Station";')
        self.fact_disruption = self.sql_extracter.extract('SELECT * FROM "Disruptions";')
        return self.sql_extracter.run()
    
    def _transform(self):
        try:
            # Transform cause
            print("Transformed causes")
            # Transform dates
            print("Transformed dates")
            # Transform station
            print("Transformed stations")
            # Transform Fact
            print("Transformed Fact")
            # Transform line_station
            print("Transformed lines")

            print("Transformation Finished")
        except Exception as e:
            print(e)
            raise e
        