#  Test creation of database table and mapping towards it.
from python import SQLHandler

def test_loading_pandas_into_custom_defined_table():
    """
    This function will test if a Dataframe to_Sql works on a existing
    """
    connection_string = "postgresql+psycopg2://postgres:example@host.docker.internal:5431/test"
    handler = SQLHandler(connection_string)