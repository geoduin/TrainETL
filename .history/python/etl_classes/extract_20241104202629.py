from abc import ABC, abstractmethod

class Extract(ABC):
    def __init__(self, source):
        super().__init__()
        self.source = source

    @abstractmethod
    def run(self):
        pass