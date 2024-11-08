from .etl_abstract import AbstractETL
from abc import ABC, abstractmethod
from pandas import DataFrame
from datetime import datetime

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

    def run(self, data: DataFrame) -> DataFrame:

        data["duration"] = data.apply(lambda row: self._calculate_duration(row['startdate'], row["enddate"]))
        return super().run()
    
    def _calculate_duration(self, start: datetime, end: datetime) -> int:
        duration = end - start
        return divmod(duration, 60)

    def _fill_current_date(self, data: DataFrame) -> DataFrame:
        """
        Fills in currentdate
        """
        return data