from airflow import Dataset
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow import AirflowException
import pandas as pd 
from python import RawPipeline, CSVExtract, SQLLoad, SQLHandler

# Hier zal nog gekeken worden of dit niet anders kan.
# station_extracter = CSVExtract("include/disruptions/disruptions-<XX>.csv")
# disruption_extracter = CSVExtract("include/disruptions/disruptions-<XX>.csv", station_extracter)
# distance_extracter = CSVExtract("include/disruptions/disruptions-<XX>.csv", disruption_extracter)

class RawDataDAG:
    def __init__(self, dag_id, start_date, schedule_interval, raw_pipeline, sqlhandler: SQLHandler):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Astro", "retries": 3},
            catchup=False,
            tags=["example"]
        )
        self.pipeline = raw_pipeline
        self.sqlhandler = sqlhandler

    def create_dag(self):
        with self.dag:

            date_dimension_task = PythonOperator(
                task_id="create_date_tables",
                python_callable=self.create_datedimension_tables
            )

            python_task = PythonOperator(
                task_id="extract_raw_data",
                python_callable=self.pipeline.run
            )
            date_dimension_task >> python_task

        return self.dag

    def create_datedimension_tables(self):
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
        
        succeeded = self.sqlhandler.run_raw_query(dim_datetime_query)

        if(not succeeded):
            raise AirflowException("Creation tables have failed")