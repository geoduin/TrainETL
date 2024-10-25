from abc import ABC, abstractmethod

class Pipeline(ABC):
    """
    The pipeline class will function as the interface every other pipeline will inherit,
    with the aim of improving modularity and thereby maintainability of the code.
    """
    def __init__(self):
        super().__init__()

    @abstractmethod
    def run(self)-> bool:
        pass