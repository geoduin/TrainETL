from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

import pandas as pd 
from raw_pipeline.raw_data_dag import RawDataDAG

# Define the basic parameters of the DAG, like schedule and start_date
dag2 = RawDataDAG("test_etl", start_date=datetime(2024, 11, 4), schedule_interval="@daily")

dag2.create_dag()