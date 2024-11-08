from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL
from pandas import DataFrame, read_csv

class Extract(AbstractETL):

    next: AbstractETL

    def __init__(self, next_step: AbstractETL = None):
        self.next = next_step

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def extract(self, file_path: str = None) -> DataFrame:
        pass

class CSVExtract(Extract):

    def __init__(self, next_step = None):
        super().__init__(next_step)

    def run(self):
        # Execute this code.
        return self.get_data()
    
    def apply_change(self, **change):
        return super().apply_change(**change)
    
    def extract(self, file_path:str = None) -> DataFrame:
        if(not file_path):
            raise ValueError("File path must be inserted")
        data = read_csv(file_path)
        return data
    

class SQLExtracter(Extract):
    connection_string:str
    def __init__(self, connection:str, next_step = None, ):
        super().__init__(next_step)
        self.connection_string = connection
