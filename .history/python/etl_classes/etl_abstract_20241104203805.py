from abc import ABC, abstractmethod

class AbstractETL(ABC):
    
    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def apply_change(self, **change):
        pass