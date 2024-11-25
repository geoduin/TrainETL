from .etl_abstract import AbstractETL
from abc import abstractmethod
from pandas import DataFrame
from datetime import datetime
from python import DateKeyHandler, ColumnSplitter, StringRemover
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
    def __init__(self, date_key_converter: DateKeyHandler, column_splitter: ColumnSplitter, remover: StringRemover):
        self.dateconverter = date_key_converter
        self.splitter = column_splitter
        self.convertion_pipeline = None
        self.remover = remover

    def run(self, data):
        # Transform disruption table
        disruption_table: DataFrame = self.convertion_pipeline.disruption
        cause_table: DataFrame = self.convertion_pipeline.cause_data
        station_table: DataFrame = self.convertion_pipeline.station_data
        # Apply id to cause_table
        cause_table["cause_id"] = range(1, len(cause_table) + 1)
        # Join disruption with
        disruption_table = disruption_table.merge(cause_table, how='left', on=['cause_en', 'statistical_cause_en', 'cause_group'])
        
        # Convert datetime to datekeys
        disruption_table["start_time"] = disruption_table.apply(lambda row: self.dateconverter.convert_datetime_to_key(row["start_time"]), axis=1)
        disruption_table["end_time"] = disruption_table.apply(lambda row: self.dateconverter.convert_datetime_to_key(row["end_time"]), axis=1)

        # Create line_station table
        line_station_table = disruption_table[["rdt_id", "rdt_lines_id", "rdt_lines"]]
        # Split rdt_lines_id and rdt_lines into their own records
        line_station_table = self.splitter.split_column_vertically(line_station_table, ["rdt_lines_id", "rdt_lines"], ",")
        # Split rdt_lines into their own columns Begin and EndStation
        line_station_table = self.splitter.split_columns(line_station_table, "rdt_lines", "-", ["begin_station_w", "end_station_w"])
        # Clean stations with HSL
        self.remover.remove(line_station_table, "(HSL)", ["begin_station_w", "end_station_w"])
        # Join begin and endstation with Station table and only get the id.
        line_begin_station_joined = line_station_table.merge(station_table, left_on="begin_station_w", right_on="name_long")
        line_end_station_joined = line_station_table.merge(line_begin_station_joined, left_on="end_station_w", right_on="name_long")
        # Drop unnecessary columns.
        disruption_table = disruption_table.drop(columns=["cause_nl", "cause_en", "statistical_cause_nl", "statistical_cause_en", "cause_group", "ns_lines", "rdt_lines", "rdt_lines_id", "rdt_station_names", "rdt_station_codes"])
        cause_table.columns = ["cause", "statistical_cause", "cause_group", "cause_id"]

        # Apply changed disruption table pipeline
        self.convertion_pipeline.disruption = disruption_table
        self.cause_data = cause_table
        return super().run(data)
    
    def apply_change(self, **change):
        self.convertion_pipeline = change.get("pipeline_instance", None)
        if self.convertion_pipeline is None: 
            raise ValueError("Needs a convertion pipeline")
        return super().apply_change(**change)