from abc import ABC, abstractmethod

class AbstractETL(ABC):
    
    @abstractmethod
    def run():
        pass