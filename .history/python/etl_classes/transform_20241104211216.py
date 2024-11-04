from etl_abstract import AbstractETL
from abc import ABC, abstractmethod

class Transform(AbstractETL):
    next: AbstractETL

    def __init__(self):
        super().__init__()

    @abstractmethod
    def run(self):
        pass