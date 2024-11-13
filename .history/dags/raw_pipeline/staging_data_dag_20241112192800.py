from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd 
import logging

class StagingDataDAG:
    
    def __init__(self, dag_id, start_date, schedule_interval, raw_pipeline):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Astro", "retries": 3},
            catchup=False,
            tags=["example"]
        )
        self.pipeline = raw_pipeline

    def create_dag(self):
        with self.dag:
            # First clear all data
            create_tables = PythonOperator(
                task_id="Create_tables",
                python_callable=self.create_tables
                )

            # RUN ETL Pipeline from raw to staging
            python_task = PythonOperator(
                task_id="ETL_To_Staging_DB",
                python_callable=self.pipeline.run
            )

            create_tables >> python_task

        return self.dag
    
    def assign_adhoc_connection(self, connection):
        self.ad_hoc = create_engine(connection)

    # NOTE: Will need to be fixed.
    def create_tables(self):
        """
        This method will create a datetime dimension table for the datawarehouse
        """
        dim_datetime_query = """
            DROP TABLE IF EXISTS Dim_DateTime;
            CREATE TABLE Dim_DateTime (
                id BIGINT PRIMARY KEY,
                year SMALLINT,
                month SMALLINT,
                day SMALLINT,
                hour SMALLINT,
                minute SMALLINT
            );

            INSERT INTO Dim_DateTime (id, year, month, day, hour, minute)
                SELECT 
                    TO_CHAR(d, 'YYYYMMDDHH24MI')::BIGINT AS id,
                    EXTRACT(YEAR FROM d)::SMALLINT AS year,
                    EXTRACT(MONTH FROM d)::SMALLINT AS month,
                    EXTRACT(DAY FROM d)::SMALLINT AS day,
                    EXTRACT(HOUR FROM d)::SMALLINT AS hour,
                    EXTRACT(MINUTE FROM d)::SMALLINT AS minute
                FROM generate_series('2023-01-01 00:00:00'::TIMESTAMP, '2025-12-31 23:59:00'::TIMESTAMP, '1 minute') AS d;
            """
        logging.info(dim_datetime_query)
        
    
    # NOTE: Will need to be fixed.
    def drop_data(self):
        delete_query = """
            DELETE FROM 'Disruptions';
            DELETE FROM 'Stations';
            DELETE FROM 'Services';
            """
        logging.info(delete_query)
        #pd.read_sql_query(delete_query, self.ad_hoc)