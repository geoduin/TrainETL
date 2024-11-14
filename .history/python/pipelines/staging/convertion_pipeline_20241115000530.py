from ..pipeline import Pipeline
from python import SQLHandler, Transform, Extract, Load
from pandas import DataFrame

import logging

class ConvertionPipeline(Pipeline):
    sql_handler: SQLHandler
    disruption: DataFrame
    station_data: DataFrame
    cause_data: DataFrame 
    line_station: DataFrame

    extracter: Extract
    convertion_transformer: Transform
    loader: Load

    def __init__(self, sql_handler: SQLHandler):
        self.sql_handler = sql_handler

    def run(self):
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
                cause_group
            )
        """

        station_table = """
            
            CREATE TABLE "Stations" (
                id BIGINT PRIMARY KEY,
                code TEXT,
                uic BIGINT,
                name TEXT,
                name_medium TEXT,
                name_long TEXT,
                slug TEXT,
                country TEXT,
                TYPE TEXT,
                geo_lat DOUBLE,
                geo_lng DOUBLE
            )
        """

        line_table = """
            
            CREATE TABLE "Line_Disruption" (
                rdt_id BIGINT,
                rdt_lines_id BIGINT,
                rdt_line TEXT,
                begin_station BIGINT,
                end_station BIGINT
            )
        """

        disruption_table = """
            
            CREATE TABLE "Disruption" (
                rdt_id BIGINT PRIMARY KEY,
                duration_minutes DOUBLE,
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
            DROP TABLE IF EXISTS "Disruption";
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