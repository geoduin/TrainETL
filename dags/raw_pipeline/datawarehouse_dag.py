from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from python import DataWarehousePipeline

class DataWarehouseDAG:
    """
    This DAG is responsible for transfering data from converted database to datawarehouse.
    This will not delete any data, but only appends and updates data. 
    """
    def __init__(self, dag_id, start_date, schedule_interval, pipeline: DataWarehousePipeline):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Geoduin", "retries": 3},
            catchup=False,
            tags=["example"]
        )
        self.pipeline = pipeline

    def create_dag(self):
        with self.dag:
            python_task = PythonOperator(
                task_id="delete_tables",
                python_callable=self.pipeline.delete_temp_tables
            )

            create_tables_if_not_exists = PythonOperator(
                task_id="create_operational_tables_if_not_exists",
                python_callable=self.pipeline._create_tables_ifnotexists
            )

            run_pipeline = PythonOperator(
                task_id="run_pipelines",
                python_callable=self.pipeline.run
            )
            
            create_tables_if_not_exists >> python_task >> run_pipeline
        return self.dag
    
    def default_behavior(self):
        print("Create temporal tables")
        

                                
