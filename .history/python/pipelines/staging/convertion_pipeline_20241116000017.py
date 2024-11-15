from ..pipeline import Pipeline
from python import SQLHandler, Transform, Extract, Load, SQLLoad, SQLExtracter
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

    def __init__(self, sql_handler: SQLHandler, sql_extracter: SQLExtracter, sql_loader: SQLLoad):
        self.sql_handler = sql_handler
        self.extracter = sql_extracter
        self.loader = sql_loader

    def run(self):
        # One to one extract and load, without any transformation
        # Note need to be tested.
        logging.info("Extract data from staging database")
        self.dim_date = self.extracter.extract('SELECT * FROM "Dim_DateTime";')
        self.cause_data = self.extracter.extract("""
                                                 SELECT DISTINCT "cause_en" as "cause", "statistical_cause_en" AS "statistical_cause", "cause_group" 
                                                 FROM "Disruptions";
                                                 """)
        self.station_data = self.extracter.extract('SELECT * FROM "Stations";')
        # Disruption, Line_Station need to be transformed in Python.
        logging.info("Transform data")

        # Load data
        logging.info("Load data into convertion database")
        self.loader.load_data(self.dim_date, "Dim_DateTime")
        self.loader.load_data(self.cause_data, "Cause")
        self.loader.load_data(self.station_data, "Stations")
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
            CREATE TABLE "Cause" (
                cause_id SERIAL PRIMARY KEY,
                cause VARCHAR(100),
                statistical_cause VARCHAR(100),
                cause_group VARCHAR(100)
            )
        """

        station_table = """
            
            CREATE TABLE "Stations" (
                id BIGINT PRIMARY KEY,
                code VARCHAR(100),
                uic BIGINT,
                name VARCHAR(100),
                name_medium VARCHAR(100),
                name_long VARCHAR(100),
                slug VARCHAR(100),
                country VARCHAR(100),
                TYPE VARCHAR(100),
                geo_lat DOUBLE PRECISION,
                geo_lng DOUBLE PRECISION
            )
        """

        line_table = """
            
            CREATE TABLE "Line_Disruption" (
                rdt_id BIGINT,
                rdt_lines_id BIGINT,
                rdt_line VARCHAR(100),
                begin_station BIGINT,
                end_station BIGINT
            )
        """

        disruption_table = """
            
            CREATE TABLE "Disruptions" (
                rdt_id BIGINT PRIMARY KEY,
                duration_minutes INTEGER,
                cause_id BIGINT,
                start_time BIGINT,
                end_time BIGINT
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
            DROP TABLE IF EXISTS "Line_Disruption";
            DROP TABLE IF EXISTS "Disruptions";
            DROP TABLE IF EXISTS "Dim_DateTime";
            DROP TABLE IF EXISTS "Cause";
            DROP TABLE IF EXISTS "Stations";
        """
        try:

            logging.info("Start dropping data")
            self.sql_handler.run_raw_query(drop_tables)
        except Exception as m:
            logging.error(m)
            logging.error("SQL Query went wrong")