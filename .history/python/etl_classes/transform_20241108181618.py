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

class MissingEndDateTransformer(Transform):
    """
    This class will impute the missing date values based of business rules.
    - In this case, if no endtime was present the current datetime will be used as impute data.
    - Since duration is dependant on start and enddate time, this will calculate the duration in minutes.
    """

    def run(self, data) -> DataFrame:
        return super().run()
    
    def _calculate_duration(self, data: DataFrame) -> int:
        return 0