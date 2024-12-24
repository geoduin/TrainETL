from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from python import DataWarehousePipeline, SnowflakeHandler
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine

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
            default_args={"owner": "Geoduin", "retries": 3},
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
        print("Create temporal tables")
        temp_table_date = Table("Temp_DateTime", 
                                MetaData(), 
                                Column("id", Integer, primary_key=True), 
                                Column("date", String),
                                Column("time", String),
                                Column("day", String),
                                Column("month", String),
                                Column("year", String),
                                Column("hour", String),
                                Column("minutes", String),
                                Column("seconds", String))
        temp_table_cause = Table("Temp_Cause", 
                                MetaData(), 
                                Column("cause_id", Integer, primary_key=True), 
                                Column("cause", String),
                                Column("cause_group", String),
                                Column("statistical_cause", String))
        temp_table_station = Table("Temp_Stations", 
                                MetaData(), 
                                Column("id", Integer, primary_key=True), 
                                Column("uic", String),
                                Column("name_short", String),
                                Column("name_medium", String),
                                Column("name_long", String),
                                Column("slug", String),
                                Column("country", String),
                                Column("type", String),
                                Column("geo_lat", String),
                                Column("geo_lng", String))
        temp_table_lines = Table("Temp_Station_Lines", 
                                MetaData(), 
                                Column("rdt_id", Integer, primary_key=True), 
                                Column("rdt_lines_id", Integer),
                                Column("rdt_lines", Integer),
                                Column("begin_station", String),
                                Column("end_station", String))
        temp_table_disruptions = Table("Temp_Disruptions", 
                                MetaData(), 
                                Column("id", Integer, primary_key=True), 
                                Column("start_time", Integer),
                                Column("end_time", Integer),
                                Column("duration_minutes", String),
                                Column("cause_id", String))
                                
