from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL
from pandas import DataFrame

class Extract(AbstractETL):

    next: AbstractETL

    def __init__(self, source, next_step: AbstractETL = None):
        self.source = source
        self.next = next_step

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def get_data(self):
        pass

class CSVExtract(Extract):

    def __init__(self, source, next_step = None):
        super().__init__(source, next_step)

    def run(self):
        # Execute this code.
        return super().run()
    
    def get_data(self) -> DataFrame:
        return super().get_data()