from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

class WarehouseDAG:
    """
    This DAG is responsible for transfering data from converted database to datawarehouse.
    This will not delete any data, but only appends and updates data. 
    """
    def __init__(self, dag_id, start_date, schedule_interval, pipeline):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Xin", "retries": 3},
            catchup=False,
            tags=["example"]
        )
        self.pipeline = pipeline

    def create_dag(self):
        with self.dag:
            python_task = PythonOperator(
                task_id="run_warehouse_etl",
                python_callable=self.pipeline.run
            )
            
        return self.dag