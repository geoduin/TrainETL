from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

import pandas as pd 
from raw_pipeline.raw_data_dag import RawDataDAG
from raw_pipeline.staging_data_dag import StagingDataDAG
from raw_pipeline.convertion_data_dag import ConvertionDataDag

from python import RawPipeline, CSVExtract, SQLLoad, CleaningPipeline, SQLExtracter, MissingEndDateTransformer, SQLHandler, ConvertionPipeline

raw_connection = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/postgres"
staging_connection = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/staging"

csv_extracter = CSVExtract()
sql_extracter = SQLExtracter(raw_connection)

sql_loader = SQLLoad(raw_connection)
sql_staging_loader = SQLLoad(staging_connection)

missing_values_transformer = MissingEndDateTransformer()
sql_handler = SQLHandler(raw_connection)

raw_pipeline = RawPipeline(csv_extracter=csv_extracter, sql_loader=sql_loader)
clean_pipeline = CleaningPipeline(sql_extracter=sql_extracter, sql_loader=sql_staging_loader, transformer=missing_values_transformer)
convertion_pipeline = ConvertionPipeline()

# Define the basic parameters of the DAG, like schedule and start_date
dag2 = RawDataDAG("test_python_function", start_date=datetime(2024, 11, 5), schedule_interval="@daily", raw_pipeline=raw_pipeline, sqlhandler=sql_handler)
dag_staging = StagingDataDAG("clean_up_data", start_date=datetime(2024, 11, 8), schedule_interval="@daily", raw_pipeline=clean_pipeline)
dag_convertion = ConvertionDataDag("convert_data", start_date=datetime(2024, 11, 5), schedule_interval="@daily", raw_pipeline=convertion_pipeline)

dag2.create_dag()
dag_staging.create_dag()
dag_convertion.create_dag()