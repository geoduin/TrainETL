from ..pipeline import Pipeline
from python import Load, Extract, Transform, SnowflakeHandler, SCDType1Transformer
from pandas import DataFrame
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float

class DataWarehousePipeline(Pipeline):
    sql_extracter: Extract
    sql_load: Load
    transformers: SCDType1Transformer

    # Dim tables
    dim_date: DataFrame
    dim_cause: DataFrame
    dim_line_station: DataFrame
    dim_stations: DataFrame
    # Fact tables
    fact_disruption: DataFrame

    def __init__(self, sql_extracter: Extract, sql_load: Load, transformers: SCDType1Transformer, snowflake_handler: SnowflakeHandler):
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
        Table("Temp_DateTime", 
            METADATA, 
            Column("id", Integer, primary_key=True), 
            Column("year", Integer),
            Column("month", Integer),
            Column("day", Integer),
            Column("hour", Integer),
            Column("minute", Integer),
            prefixes=['TEMPORARY'],)
        Table("Temp_Cause", 
            METADATA, 
            Column("cause_id", Integer, primary_key=True), 
            Column("cause", String),
            Column("cause_group", String),
            Column("statistical_cause", String),
            prefixes=['TEMPORARY'],)
        Table("Temp_Stations", 
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
            Column("geo_lat", Float),
            Column("geo_lng", Float),
            prefixes=['TEMPORARY'])    
        Table("Temp_Station_Lines", 
            METADATA, 
            Column("rdt_id", Integer, primary_key=True), 
            Column("rdt_lines_id", String),
            Column("rdt_lines", String),
            Column("begin_station", Integer),
            Column("end_station", Integer),
            prefixes=['TEMPORARY'])   
        Table("Temp_Disruptions", 
            METADATA, 
            Column("id", Integer, primary_key=True), 
            Column("start_time", Integer),
            Column("end_time", Integer),
            Column("duration_minutes", Integer),
            Column("cause_id", Integer),
            prefixes=['TEMPORARY'])
        METADATA.create_all(local_engine, checkfirst=True)
    
    def _create_tables_ifnotexists(self):
        print("Create tables if not exists")
        local_engine = self.snowflake_handler.engine
        local_connection = self.snowflake_handler.get_connection()
        METADATA = MetaData(bind=local_connection)
        dim_dates = Table("Dim_DateTime", 
            METADATA, 
            Column("id", Integer, primary_key=True), 
            Column("year", String),
            Column("month", String),
            Column("day", String),
            Column("hour", String),
            Column("minute", String))
        dim_cause = Table("Dim_Cause", 
            METADATA, 
            Column("cause_id", Integer, primary_key=True), 
            Column("cause", String),
            Column("cause_group", String),
            Column("statistical_cause", String))
        dim_stations = Table("Dim_Stations", 
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
            Column("geo_lat", Integer),
            Column("geo_lng", Integer))    
        dim_lines = Table("Dim_Station_Lines", 
            METADATA, 
            Column("rdt_id", Integer, ForeignKey("Fact_Disruptions.id")), 
            Column("rdt_lines_id", String),
            Column("rdt_lines", String),
            Column("begin_station", Integer, ForeignKey("Dim_Stations.id")),
            Column("end_station", Integer, ForeignKey("Dim_Stations.id")))   
        fact_disruptions = Table("Fact_Disruptions", 
            METADATA, 
            Column("id", Integer, primary_key=True), 
            Column("start_time", Integer, ForeignKey("Dim_DateTime.id")),
            Column("end_time", Integer, ForeignKey("Dim_DateTime.id")),
            Column("duration_minutes", Integer),
            Column("cause_id", Integer, ForeignKey("Dim_Cause.cause_id")))
        
        METADATA.create_all(local_engine, checkfirst=True)
        
    def _load(self):
        """
        Load data from the staging database
        """
        config = {"if_exist": "replace", "chunksize": 16000}
        config_other = {"if_exist": "replace"}
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
            self.transformers.merge_data(self.dim_cause, "Temp_Cause", "Dim_Cause", "cause_id", "cause_id")
            print("Transformed causes")
            # Transform dates
            # self.transformers.merge_data(self.dim_date, "Temp_DateTime", "Dim_DateTime")
            print("Transformed dates")
            # Transform station
            print("Transformed stations")
            self.transformers.merge_data(self.dim_stations, "Temp_Stations", "Dim_Stations", "id", "id")
            # Transform Fact
            print("Transformed Fact")
            self.transformers.merge_data(self.fact_disruption, "Temp_Disruptions", "Fact_Disruptions", "rdt_id", "id")
            # Transform line_station
            print("Transformed lines")
            self.transformers.merge_data(self.dim_line_station, "Temp_Station_Lines", "Dim_Station_Lines", "rdt_id", "rdt_id")

            print("Transformation Finished")
        except Exception as e:
            print(e)
            raise e
        