import pandas as pd
import os 
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
                         "start_time": ["2024-11-06 18:20:28", "2024-11-07 18:20:28", "2024-11-7 18:20:28"]}, 
                         dtype={'end_time': 'datetime64[ns]', 'start_time': 'datetime64[ns]'})
    result = transformer._fill_current_date(data)

    assert result["start_time"][0] == "2024-11-06 18:20:28"
    assert result["start_time"][2] == "2024-11-07 18:20:28"
    assert result["start_time"][2] == "2024-11-7 18:20:28"
    assert result["end_time"][0] == "2024-11-08 18:35:28"
    assert result["end_time"][1] == current_datetime
    assert result["end_time"][2] == current_datetime
    