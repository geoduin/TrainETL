from .etl_abstract import AbstractETL
from abc import ABC, abstractmethod
from pandas import DataFrame

class Transform(AbstractETL):
    next: AbstractETL

    def __init__(self):
        super().__init__()

    @abstractmethod
    def run(self, data: DataFrame) -> DataFrame:
        pass

class MissingDateTransformer(Transform):
    """
    This class will impute the missing date values based of business rules.
    - In this case, if no endtime was present the current datetime will be used as impute data.
    """

    def run(self, data) -> DataFrame:
        return super().run()