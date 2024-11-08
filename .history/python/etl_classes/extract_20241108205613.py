from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL
from pandas import DataFrame, read_csv
from sqlalchemy import create_engine

class Extract(AbstractETL):

    next: AbstractETL

    def __init__(self, next_step: AbstractETL = None):
        self.next = next_step

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def extract(self, query: str = None) -> DataFrame:
        pass

class CSVExtract(Extract):

    def __init__(self, next_step = None):
        super().__init__(next_step)

    def run(self):
        # Execute this code.
        return self.get_data()
    
    def apply_change(self, **change):
        return super().apply_change(**change)
    
    def extract(self, query:str = None) -> DataFrame:
        if(not query):
            raise ValueError("File path must be inserted")
        data = read_csv(query)
        return data
    

class SQLExtracter(Extract):
    connection_string:str

    def __init__(self, connection:str, next_step = None):
        super().__init__(next_step)
        self.connection_string = connection
        self.engine = create_engine(self.connection_string)

    def run(self):
        return super().run()
    
    def extract(self, query = None):
        if(not query):
            raise ValueError("File path must be inserted")
        return super().extract(query)
