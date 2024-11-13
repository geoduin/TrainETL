from sqlalchemy import create_engine

class SQLHandler(object):

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        pass

    def switch_connection(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def run_raw_query(self, query: str):
        try:

