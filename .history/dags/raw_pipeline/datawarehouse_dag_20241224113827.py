from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from python import DataWarehousePipeline, SnowflakeHandler

class DataWarehouseDAG:
    """
    This DAG is responsible for transfering data from converted database to datawarehouse.
    This will not delete any data, but only appends and updates data. 
    """
    def __init__(self, dag_id, start_date, schedule_interval, pipeline: DataWarehousePipeline, snowflake: SnowflakeHandler):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Xin", "retries": 3},
            catchup=False,
            tags=["example"]
        )
        self.pipeline = pipeline
        self.snowflake_handler = snowflake

    def create_dag(self):
        with self.dag:
            python_task = PythonOperator(
                task_id="create_databases",
                python_callable=self.default_behavior
            )

            python_task2 = PythonOperator(
                task_id="run_pipelines",
                python_callable=self.pipeline.run
            )
            
            python_task >> python_task2
        return self.dag
    
    def default_behavior(self):
        print("This is the default behavior of the WarehouseDAG")