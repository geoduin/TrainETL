from abc import ABC, abstractmethod
from .etl_abstract import AbstractETL
import sqlalchemy as sql
from sqlalchemy import create_engine
from pandas import DataFrame # type: ignore

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
        table_schema =  config.get("dtypes",None)
            
        # Subject to change
        data.to_sql(table, self.connection, if_exists=config.get("if_exist", "fail"), dtype=table_schema, index=False)

class SnowflakeLoad(Load):

    def __init__(self, target: str, next_step = None, **config):
        print(target)
        self.connection = create_engine(target, config={ "database": "TreinDatabase", "schema": "PUBLIC"})

    def run(self):
        return super().run()
    
    def load_data(self, data: DataFrame, table:str, **config):
        try:
            data.to_sql(table, self.connection, if_exists=config.get("if_exist", "fail"), index=False)
        except Exception as e:
            print(e)
            raise e
    
    def apply_change(self, **change):
        return super().apply_change(**change)