from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator, HttpOperator
from airflow.decorators import dag, task
from python import DataWarehousePipeline

class DeliveryDag():
    
    def __init__(self, dag_id, start_date, schedule_interval):
        self.dag = DAG(
            dag_id = dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args={"owner": "Geoduin", "retries": 2},
            catchup=False,
            tags=["example"]
        )

    def create_dag(self):
        with self.dag:
            check_data = PythonOperator(
                task_id="check_data",
                python_callable=self.default_behavior
            )

            data_science_delivery = SimpleHttpOperator(
                task_id="deliver_to_data_scientists",
                http_conn_id='data_science',
                endpoint='/load/data_scientists',
                method='PUT',
                headers={"Content-Type": "application/json"},
                dag=self.dag,
            )

            data_analyst_delivery = SimpleHttpOperator(
                task_id="deliver_to_data_analist",
                http_conn_id='data_analist',
                endpoint='/load/data_analyst',
                method='PUT',
                headers={"Content-Type": "application/json"},
                dag=self.dag,
            )
            
            data_science_delivery << check_data >> data_analyst_delivery
        return self.dag
    
    def default_behavior(self):
        print("Create temporal tables")