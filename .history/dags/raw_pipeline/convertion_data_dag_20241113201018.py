from airflow import DAG
from airflow.operators.python import PythonOperator
from python import ConvertionPipeline
import logging

class ConvertionDataDag:

    def __init__(self, dag_id, start_date, schedule_interval, raw_pipeline: ConvertionPipeline):
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

            define_tables = PythonOperator(
                task_id="run_table_creation",
                python_callable=self.pipeline.create_tables
            )

            python_task = PythonOperator(
                task_id="run_convertion_steps",
                python_callable=self.pipeline.run
            )
            
            define_tables >> python_task

        return self.dag
    
    def run_prints(self):
        logging.info("Hello world")
    
    def run_prints2(self):
        logging.info("Hello world 2")