from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL
from pandas import DataFrame, read_csv, read_sql
from sqlalchemy import create_engine
import numpy as np

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
        return super().run()
    
    def apply_change(self, **change):
        return super().apply_change(**change)
    
    def extract(self, query:str = None) -> DataFrame:
        if(not query):
            raise ValueError("File path must be inserted")
        data = read_csv(query, index_col=True)
        return data
    

class SQLExtracter(Extract):
    connection_string:str

    def __init__(self, connection:str, next_step = None):
        super().__init__(next_step)
        self.connection_string = connection
        self.engine = create_engine(self.connection_string)

    def run(self):
        return super().run()
    
    def apply_change(self, **change):
        return super().apply_change(**change)
    
    def extract(self, query = None):
        try:
            if(not query):
                raise ValueError("File path must be inserted")
            return read_sql(query, con=self.engine)
        except LookupError as m:
            print(m)
            raise ValueError("Read has failed")