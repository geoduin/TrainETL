from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

import pandas as pd 
from raw_pipeline.raw_data_dag import RawDataDAG
from python import RawPipeline, CSVExtract, SQLLoad

raw_pipeline = RawPipeline()

# Define the basic parameters of the DAG, like schedule and start_date
dag2 = RawDataDAG("test_python_function", start_date=datetime(2024, 11, 5), schedule_interval="@daily", raw_pipeline=raw_pipeline)
dag2.create_dag()