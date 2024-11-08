import pandas as pd
import os 
import logging
from os import listdir
from os.path import isfile, join
from datetime import datetime
from python import MissingEndDateTransformer
base_dir = os.path.dirname(os.path.realpath(__file__))


# Test case tables are created

# Test case creation of cause dimension tables goes well

# Test case creation of line-disruption data goes well.

# Test case datetime-based id is created.

# Test case fact tables and dimension tables

# Test case splitting multiple lines from column into their own

# Test case impute empty data.
def test_missing_end_time_value():
    transformer = MissingEndDateTransformer()
    current_datetime = datetime.now()

    data = pd.DataFrame({"end_time": ["2024-11-08 18:35:28", None, None], 
                         "start_time": ["2024-11-06 18:20:28", "2024-11-07 18:20:28", "2024-11-7 18:20:28"]})
    
    data["start_time"] = pd.to_datetime(data["start_time"])
    data["end_time"] = pd.to_datetime(data["end_time"])

    data['end_time'] = transformer._fill_current_date(data)
    assert data.dtypes = []
    assert str(data["end_time"].iloc[0]) == "2024-11-08 18:35:28"
    assert str(data["end_time"].iloc[1]) == current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    assert str(data["end_time"].iloc[2]) == current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    