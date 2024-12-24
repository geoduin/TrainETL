from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests

from raw_pipeline.raw_data_dag import RawDataDAG
from raw_pipeline.staging_data_dag import StagingDataDAG
from raw_pipeline.convertion_data_dag import ConvertionDataDag
from raw_pipeline.datawarehouse_dag import DataWarehouseDAG
from raw_pipeline.test_dag import TestDag


from python import RawPipeline, CSVExtract, SQLLoad, CleaningPipeline, SQLExtracter, MissingEndDateTransformer, SQLHandler, ConvertionPipeline, ConvertionTransformer, DateKeyHandler, ColumnSplitter, StringRemover, DataWarehousePipeline, SnowflakeLoad, SCDType1Transformer, SCDType2Transformer

raw_connection = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/postgres"
staging_connection = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/staging"
convertion_connection = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/conversion"
datawarehouse_connection = "snowflake://Geoduin:Pass4Word@VPCUYOS-GU56680/?warehouse=TRAINWAREHOUSE&role=ACCOUNTADMIN"
# Extract classes
csv_extracter = CSVExtract()
sql_extracter = SQLExtracter(raw_connection)
sql_extracter_staging = SQLExtracter(staging_connection)

# Loaders
sql_loader = SQLLoad(raw_connection)
sql_staging_loader = SQLLoad(staging_connection)
sql_convertion_loader = SQLLoad(convertion_connection)
snowflake_loader = SnowflakeLoad(datawarehouse_connection)

# Transformers
date_converter = DateKeyHandler()
column_splitter = ColumnSplitter()
string_remover = StringRemover()
missing_values_transformer = MissingEndDateTransformer()
convertion_transformer = ConvertionTransformer(date_key_converter=date_converter, column_splitter=column_splitter, remover=string_remover)
sql_handler = SQLHandler(raw_connection)
sql_convertion_handler = SQLHandler(convertion_connection)
snowflake_scd_transformer = SCDType1Transformer()

# Pipelines
raw_pipeline = RawPipeline(csv_extracter=csv_extracter, sql_loader=sql_loader, sql_handler=sql_handler)
clean_pipeline = CleaningPipeline(sql_extracter=sql_extracter, sql_loader=sql_staging_loader, transformer=missing_values_transformer)
convertion_pipeline = ConvertionPipeline(sql_handler=sql_convertion_handler, sql_extracter=sql_extracter_staging, sql_loader=sql_convertion_loader, transformer=convertion_transformer)
data_warehouse_pipeline = DataWarehousePipeline()

# Define the basic parameters of the DAG, like schedule and start_date
test_dag = TestDag("debug_dag", start_date=datetime(2024, 11, 18), schedule_interval="@daily")
dag2 = RawDataDAG("extract_raw_data", start_date=datetime(2024, 11, 18), schedule_interval="@daily", raw_pipeline=raw_pipeline)
dag_staging = StagingDataDAG("clean_up_data", start_date=datetime(2024, 11, 18), schedule_interval="@daily", raw_pipeline=clean_pipeline)
dag_convertion = ConvertionDataDag("convert_data", start_date=datetime(2024, 11, 18), schedule_interval="@daily", raw_pipeline=convertion_pipeline)

# Load into Data Warehouse
data_warehouse_dag = DataWarehouseDAG("data_warehouse", start_date=datetime(2024, 11, 18), schedule_interval="@daily", pipeline=data_warehouse_pipeline)
# Send data to data scientists and analysts

dag2.create_dag()
dag_staging.create_dag()
dag_convertion.create_dag()
test_dag.create_dag()
data_warehouse_dag.create_dag()