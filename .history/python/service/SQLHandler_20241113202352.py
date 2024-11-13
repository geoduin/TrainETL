from sqlalchemy.sql import text
import sqlalchemy

class SQLHandler(object):

    def __init__(self, connection_string: str):
        self.engine = sqlalchemy.create_engine(connection_string)

    def switch_connection(self, connection_string: str):
        self.engine = sqlalchemy.create_engine(connection_string)
    
    def run_raw_query(self, query: str):
        try:
            cc = sqlalchemy.create_engine("")
            with cc.connect() as connect:
                connect.execute(text(query))
                connect.commit()
            return True
        except:
            raise

