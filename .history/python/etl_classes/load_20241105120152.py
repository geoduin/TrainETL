from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL
import sqlalchemy as sql
from sqlalchemy import create_engine
from sqlalchemy.connectors import Connector
from pandas import DataFrame

class Load(AbstractETL):

    next: AbstractETL

    def __init__(self, target: str, next_step: AbstractETL = None):
        self.target = target
        self.next = next_step

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def load_data(self, data, table=None, **config):
        pass

class SQLLoad(Load):
    # Can be subjected to change.
    def __init__(self, target: str, next_step = None, database = "postgres"):
        super().__init__(target, next_step)
        self.database = database
        self.connection = create_engine(target)

    def run(self):
        return super().run()
    
    def apply_change(self, **change):
        data = change.get("Data", None)
        if data:
            self.run()

        return None
    
    def load_data(self, data: DataFrame, table=None, **config):

        # Subject to change
        data.to_sql(table, self.connection, if_exists=config.get("if_exist", "fail"))