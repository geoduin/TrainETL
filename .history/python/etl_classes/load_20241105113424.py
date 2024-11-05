from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL

class Load(AbstractETL):

    next: AbstractETL

    def __init__(self, target: str, next_step: AbstractETL = None):
        self.target = target
        self.next = next_step

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def load_data(self, data, table):
        pass

class SQLLoad(Load):

    def __init__(self, target, next_step = None, database = "postgres"):
        super().__init__(target, next_step)
        self.database = database

    def run(self):
        return super().run()
    
    def apply_change(self, **change):
        data = change.get("Data", None)
        if data:
            self.run()

        return None