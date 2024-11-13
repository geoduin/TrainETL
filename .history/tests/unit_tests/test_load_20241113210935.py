#  Test creation of database table and mapping towards it.
from python import SQLHandler
from sqlalchemy import create_engine
import datetime 
import pandas as pd
connection_string = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/test"
handler = SQLHandler(connection_string)

def reset_database():
    print("Delete database")
    handler.run_raw_query('DROP TABLE "Person"')

def test_loading_pandas_into_custom_defined_table():
    """
    This function will test if a Dataframe to_Sql works on a existing
    """
    reset_database()
    connection = create_engine(connection_string)
    data =[ {"id": 1, "name": "Xin", "birthdate": datetime.datetime(2001, 1, 14), "age": 23},  {"id": 2, "name": "Dave", "birthdate": datetime.datetime(2001, 1, 13), "age": 17}]
    df = pd.DataFrame(data)
    schema = """
    CREATE TABLE "Person" (
        name VARCHAR(50),
        birthdate TIMESTAMP,
        age INTEGER
    )
"""
    handler.run_raw_query(schema)
    answer = df.to_sql("Person", con=, if_exists="append", index=False)
    assert answer == 1
    