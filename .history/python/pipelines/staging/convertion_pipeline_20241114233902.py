from ..pipeline import Pipeline
from python import SQLHandler

class ConvertionPipeline(Pipeline):
    sql_handler: SQLHandler

    def __init__(self, sql_handler: SQLHandler):
        self.sql_handler = sql_handler
        super().__init__()

    def run(self):
        return super().run()
    
    def create_tables(self):
        dim_date = """
            DROP TABLE IF EXISTS "Dim_DateTime";
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
            DROP TABLE IF EXISTS "Cause";
            CREATE TABLE "Cause"(
                cause_id BIGINT AUTOINCREMENT PRIMARY KEY,
                cause VARCHAR(100),
                cause_group
            )
        """

        station_table = """
            DROP TABLE IF EXISTS "Stations";
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
        print()
    