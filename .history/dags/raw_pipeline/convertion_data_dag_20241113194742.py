from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

class ConvertionDataDag:

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