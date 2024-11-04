from airflow import Dataset
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from pendulum import datetime
import requests

import pandas as pd 
from python import RawPipeline, CSVExtract, SQLLoad

# Hier zal nog gekeken worden of dit niet anders kan.
# station_extracter = CSVExtract("include/disruptions/disruptions-<XX>.csv")
# disruption_extracter = CSVExtract("include/disruptions/disruptions-<XX>.csv", station_extracter)
# distance_extracter = CSVExtract("include/disruptions/disruptions-<XX>.csv", disruption_extracter)

raw_pipeline = RawPipeline()

class RawDataDAG:
    def __init__(self, dag_id, start_date, schedule_interval):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False
        )

    def create_dag(self):
        with self.dag:

            python_task = PythonOperator(
                task_id="Test_python",
                python_callable=raw_pipeline.run
            )

            python_task2 = PythonOperator(
                task_id="Test_python_2",
                python_callable=self.print
            )

            python_task >> python_task2

        return self.dag
    
    def print(self):
        print("Hello world")