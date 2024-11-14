from ..pipeline import Pipeline
from python import SQLHandler
from pandas import DataFrame

import logging

class ConvertionPipeline(Pipeline):
    sql_handler: SQLHandler

    def __init__(self, sql_handler: SQLHandler):
        self.sql_handler = sql_handler
        super().__init__()

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
            
            CREATE TABLE "Cause"(
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
        logging.info("Start creation of tables")
    
    def drop_data(self):
        drop_tables = """
            DROP TABLE IF EXISTS "Line_Disruption";
            DROP TABLE IF EXISTS "Disruption";
            DROP TABLE IF EXISTS "Dim_DateTime";
            DROP TABLE IF EXISTS "Cause";
            DROP TABLE IF EXISTS "Stations";
        """
        logging.info("Start dropping data")
    