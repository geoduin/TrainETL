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
            # First load tables
            create_tables = PythonOperator(
                task_id="clear_date",
                python_callable=self.create_tables
                )
            
            create_tables = PythonOperator(
                task_id="Create_tables",
                python_callable=self.create_tables
                )

            # Then run transformation
            # Load data into
            python_task = PythonOperator(
                task_id="ETL_To_Staging_DB",
                python_callable=self.pipeline.run
            )

            create_tables >> python_task

        return self.dag

    def create_tables(self):
        return 
    """
    CREATE TABLE 
    """

    def drop_tables(self):
        return 
    """
    DROP TABLE disruptions;
    DROP TABLE stations;
    DROP TABLE services;
    """