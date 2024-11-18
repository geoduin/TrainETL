from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd 
import os 
import logging

class TestDag:

    def __init__(self, dag_id, start_date, schedule_interval):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Astro", "retries": 3},
            catchup=False,
            tags=["example"]
        )

    def create_dag(self):
        with self.dag:
            operator = PythonOperator(
                task_id="test_environmental_variables",
                python_callable=self._test_debug
            )

            test_functions = PythonOperator(
                task_id="test_functions_s",
                python_callable=self._test_function
            )

            operator
        return self.dag
    
    def _test_debug(self):
        variable = os.environ.get("IsBender", "NoBenders")
        logging.info(f"IsBender variabel is {variable}")

    def _test_function(self):
        logging.info("Do Function")
        