from .etl_abstract import AbstractETL
from abc import ABC, abstractmethod
from pandas import DataFrame
from datetime import datetime
import logging

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
        data["end_time"] = self._fill_current_date(data)
        data["duration_minutes"] = data.apply(lambda row: self._calculate_duration(row['start_time'], row["end_time"]), axis=1)
        return data
    
    def apply_change(self, **change):
        return super().apply_change(**change)
    
    def _calculate_duration(self, start: datetime, end: datetime) -> int:
        """
        Calculate difference between start and enddate
        Returns the difference in a int value
        """
        duration = end - start
        return divmod(duration.total_seconds(), 60)[0]

    def _fill_current_date(self, data: DataFrame) -> DataFrame:
        """
        Fills in currentdate
        """
        return data["end_time"].fillna(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))