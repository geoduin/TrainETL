from ..pipeline import Pipeline
from python import SQLHandler, Transform, Extract, Load, SQLLoad, SQLExtracter, ConvertionTransformer, StringRemover
from pandas import DataFrame

import logging

class ConvertionPipeline(Pipeline):
    sql_handler: SQLHandler
    disruption: DataFrame
    station_data: DataFrame
    cause_data: DataFrame 
    line_station: DataFrame
    dim_date: DataFrame

    extracter: Extract
    convertion_transformer: Transform
    loader: Load
    remover: StringRemover

    def __init__(self, sql_handler: SQLHandler, sql_extracter: SQLExtracter, sql_loader: SQLLoad, transformer: ConvertionTransformer):
        self.sql_handler = sql_handler
        self.extracter = sql_extracter
        self.loader = sql_loader
        self.convertion_transformer = transformer
        # Inserts itself into the transformer
        self.convertion_transformer.apply_change(**{"pipeline_instance": self})

    def run(self):
        # One to one extract and load, without any transformation
        # Note need to be tested.
        logging.info("Extract data from staging database")
        self.dim_date = self.extracter.extract('SELECT * FROM "Dim_DateTime";')
        self.cause_data = self.extracter.extract(""" SELECT DISTINCT "cause_en", "statistical_cause_en", "cause_group" FROM "Disruptions";""")
        self.line_station = self.extracter.extract('SELECT "rdt_id", "rdt_lines_id", "rdt_lines" FROM "Disruptions";')
        self.station_data = self.extracter.extract('SELECT * FROM "Stations";')
        self.disruption = self.extracter.extract('SELECT * FROM "Disruptions";')
        # Disruption, Line_Station need to be transformed in Python.
        logging.info("Transform data")
        self.convertion_transformer.run({})

        # Load data
        logging.info("Load data into convertion database")
        loading_config = {"if_exist": "append"}
        self.loader.load_data(self.dim_date, "Dim_DateTime",**loading_config)
        self.loader.load_data(self.cause_data, "Dim_Cause", **loading_config)
        self.loader.load_data(self.station_data, "Dim_Station",**loading_config)
        self.loader.load_data(self.disruption, "Fact_Disruptions", **loading_config)
        self.loader.load_data(self.line_station, "Dim_Station_Lines", **loading_config)
        
        return super().run()
    
    def create_tables(self):
        dim_date = """
            CREATE TABLE "Dim_DateTime" (
                id BIGINT PRIMARY KEY,
                year SMALLINT,
                month SMALLINT,
                day SMALLINT,
                hour SMALLINT,
                minute SMALLINT
            );
        """

        cause_table = """
            CREATE TABLE "Dim_Cause" (
                cause_id SERIAL PRIMARY KEY,
                cause VARCHAR(100) NOT NULL,
                statistical_cause VARCHAR(100) NOT NULL,
                cause_group VARCHAR(100) NOT NULL,
                UNIQUE(cause, statistical_cause,  cause_group)
            )
        """

        station_table = """   
            CREATE TABLE "Dim_Station" (
                id BIGINT PRIMARY KEY,
                code VARCHAR(100),
                uic BIGINT,
                name_short VARCHAR(100),
                name_medium VARCHAR(100),
                name_long VARCHAR(100),
                slug VARCHAR(100),
                country VARCHAR(100),
                type VARCHAR(100),
                geo_lat DOUBLE PRECISION,
                geo_lng DOUBLE PRECISION
            )
        """

        line_table = """
            CREATE TABLE "Dim_Station_Lines" (
                rdt_id BIGINT,
                rdt_lines_id BIGINT,
                rdt_line VARCHAR(100),
                begin_station BIGINT,
                end_station BIGINT
            )
        """

        disruption_table = """
            CREATE TABLE "Fact_Disruptions" (
                rdt_id BIGINT PRIMARY KEY,
                duration_minutes INTEGER,
                cause_id BIGINT REFERENCES Dim_Cause (cause_id),
                start_time BIGINT REFERENCES Dim_DateTime (id),
                end_time BIGINT REFERENCES Dim_DateTime (id)
            )
        """
        
        try:
            logging.info("Start creation of tables")
            self.sql_handler.run_raw_query(dim_date)
            self.sql_handler.run_raw_query(cause_table)
            self.sql_handler.run_raw_query(station_table)
            self.sql_handler.run_raw_query(disruption_table)
            self.sql_handler.run_raw_query(line_table)
        except Exception as m:
            logging.error(m)
            logging.error("SQL Query went wrong")
    
    def drop_data(self):
        drop_tables = """
            DROP TABLE IF EXISTS "Dim_Station_Lines";
            DROP TABLE IF EXISTS "Fact_Disruptions";
            DROP TABLE IF EXISTS "Dim_DateTime";
            DROP TABLE IF EXISTS "Dim_Cause";
            DROP TABLE IF EXISTS "Dim_Station";
        """
        try:

            logging.info("Start dropping data")
            self.sql_handler.run_raw_query(drop_tables)
        except Exception as m:
            logging.error(m)
            logging.error("SQL Query went wrong")