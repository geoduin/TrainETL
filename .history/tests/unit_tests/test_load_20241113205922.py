#  Test creation of database table and mapping towards it.
from python import SQLHandler
import datetime 
import pandas as pd
connection_string = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/test"
handler = SQLHandler(connection_string)

def reset_database():
    print("Delete database")

def test_loading_pandas_into_custom_defined_table():
    """
    This function will test if a Dataframe to_Sql works on a existing
    """
    reset_database()
    data =[ {"id": 1, "name": "Xin", "birthdate": datetime.datetime(2001, 1, 14), "age": 23},  {"id": 2, "name": "Dave", "birthdate": datetime.datetime(2001, 1, 13), "age": 17}]

    df = pd.DataFrame(data)
    