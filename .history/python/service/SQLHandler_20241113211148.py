from sqlalchemy import create_engine, text

class SQLHandler(object):

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def switch_connection(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def run_raw_query(self, query: str):
        try:
            with self.engine.connect() as connect:
                connect.execute(text(query))
            return True
        except:
            raise

    def get_connection(self):
        return self.engine.connect()

