from abc import ABC, abstractmethod
from etl_abstract import AbstractETL

class Extract(AbstractETL):

    next: AbstractETL

    def __init__(self, source, next_step: AbstractETL = None):
        self.source = source
        self.next = next_step

    @abstractmethod
    def run(self):
        pass