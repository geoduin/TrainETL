from .etl_abstract import AbstractETL
from abc import ABC, abstractmethod
from pandas import DataFrame
from datetime import datetime
from python import ConvertionPipeline, DateKeyHandler, ColumnSplitter
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
    
class ConvertionTransformer(Transform):
    """
    Meant to apply transformation to the dataframes
    """

    convertion_pipeline: ConvertionPipeline

    def __init__(self, date_key_converter: DateKeyHandler, column_splitter: ColumnSplitter):
        self.dateconverter = date_key_converter
        self.splitter = column_splitter
        self.convertion_pipeline = None

    def run(self, data):
        # Transform disruption table
        disruption_table = self.convertion_pipeline.disruption

        # Convert datetime to datekeys
        disruption_table["start_time"] = disruption_table.apply(lambda row: self.dateconverter.convert_datetime_to_key(row["start_time"]), axis=1)
        disruption_table["end_time"] = disruption_table.apply(lambda row: self.dateconverter.convert_datetime_to_key(row["start_time"]), axis=1)
        disruption_table["cause"] = disruption_table["cause_eng"]

        # Create line_station table
        line_station_table = disruption_table[["rdt_id", "rdt_lines_id", "rdt_lines"]]

        # Drop unnecessary columns.
        disruption_table = disruption_table.drop(columns=["cause_nl", "cause_en", "statistical_cause_en", "cause_group", "ns_lines", "rdt_lines", "rdt_lines_id", "rdt_station_names", "rdt_station_codes"])
        
        # Apply changed disruption table pipeline
        self.convertion_pipeline.disruption = disruption_table
        return super().run(data)
    
    def apply_change(self, **change):
        self.convertion_pipeline = change.get("pipeline_instance", None)
        if self.convertion_pipeline is None or type(self.convertion_pipeline) != ConvertionPipeline: 
            raise ValueError("Needs a convertion pipeline")
        return super().apply_change(**change)