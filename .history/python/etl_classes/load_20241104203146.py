from abc import ABC, abstractmethod
from etl_abstract import AbstractETL

class Load(AbstractETL):
    
    next: AbstractETL

    def __init__(self, target: str, next_step: AbstractETL = None):
        self.target = target
        self.next = next_step
        super().__init__()