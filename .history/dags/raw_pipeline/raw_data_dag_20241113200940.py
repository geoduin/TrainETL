from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd 
from python import RawPipeline

class RawDataDAG:
    def __init__(self, dag_id, start_date, schedule_interval, raw_pipeline: RawPipeline):
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
                python_callable=self.pipeline.create_datedimension_tables
            )

            python_task = PythonOperator(
                task_id="extract_raw_data",
                python_callable=self.pipeline.run
            )
            date_dimension_task >> python_task

        return self.dag