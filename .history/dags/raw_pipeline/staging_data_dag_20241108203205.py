from airflow import Dataset
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from pendulum import datetime
import requests

import pandas as pd 
from python import RawPipeline, CSVExtract, SQLLoad

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
            clear_data = PythonOperator(
                task_id="clear_data",
                python_callable=self.drop_data
                )
            
            create_tables = PythonOperator(
                task_id="Create_tables",
                python_callable=self.create_tables
                )

            # RUN ETL Pipeline from raw to staging
            python_task = PythonOperator(
                task_id="ETL_To_Staging_DB",
                python_callable=self.pipeline.run
            )

            clear_data >> create_tables >> python_task

        return self.dag

    def create_tables(self):
        return 
    """
    CREATE TABLE stations ()
    CREATE TABLE disruptions ()
    CREATE TABLE services ()
    """

    def drop_data(self):
        return 
    """
    DELETE FROM disruptions;
    DELETE FROM stations;
    DELETE FROM services;
    """