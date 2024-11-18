import pandas as pd
import os 
from unittest.mock import patch

from datetime import datetime
from python import MissingEndDateTransformer, ColumnSplitter
base_dir = os.path.dirname(os.path.realpath(__file__))

# Test case impute empty data.
def test_missing_end_time_value():
    transformer = MissingEndDateTransformer()
    current_datetime = datetime.now()

    data = pd.DataFrame({"end_time": ["2024-11-08 18:35:28", None, None], 
                         "start_time": ["2024-11-06 18:20:28", "2024-11-07 18:20:28", "2024-11-7 18:20:28"]})
    
    data["start_time"] = pd.to_datetime(data["start_time"])
    data["end_time"] = pd.to_datetime(data["end_time"])

    data['end_time'] = transformer._fill_current_date(data) 
    assert str(data["end_time"].iloc[0]) == "2024-11-08 18:35:28"
    assert str(data["end_time"].iloc[1]) == current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    assert str(data["end_time"].iloc[2]) == current_datetime.strftime("%Y-%m-%d %H:%M:%S")

def test_duration_calculation():
    transformer = MissingEndDateTransformer()
    data = pd.DataFrame({ "start_time": ["2024-11-06 18:20:28", "2024-11-07 18:20:28", "2024-11-7 18:20:28"],
                         "end_time": ["2024-11-06 18:35:28", "2024-11-07 19:20:28", "2024-11-8 20:20:28"]})
    
    data["start_time"] = pd.to_datetime(data["start_time"])
    data["end_time"] = pd.to_datetime(data["end_time"])

    result = transformer._calculate_duration(data["start_time"].iloc[0], data["end_time"].iloc[0])
    result2 = transformer._calculate_duration(data["start_time"].iloc[1], data["end_time"].iloc[1])
    result3 = transformer._calculate_duration(data["start_time"].iloc[2], data["end_time"].iloc[2])
    
    assert result == 15
    assert result2 == 60
    assert result3 == 1560

def test_cleanup_missing_values():
    with patch.object(MissingEndDateTransformer, '_fill_current_date') as mock_fill_date:
        mock_fill_date.return_value = datetime(2024, 11, 7, 19, 30, 32)
        transformer = MissingEndDateTransformer()
        data = pd.DataFrame({ 
        "start_time": ["2024-11-06 18:20:28", "2024-11-07 18:20:28", "2024-11-7 18:20:28"],
        "end_time": ["2024-11-06 18:35:28", None, None],
        "duration": [15, None, None]
        })

        data["start_time"] = pd.to_datetime(data["start_time"])
        data["end_time"] = pd.to_datetime(data["end_time"])

        result = transformer.run(data)

        assert len(result) == 3
        assert result["duration"].iloc[1] == 70
        assert str(result["end_time"].iloc[1]) == "2024-11-07 19:30:32"

def test_column_splitter_incorrect_amount_columns():
    splitter = ColumnSplitter()
    record_one = {"rdt_id": "40500", "rdt_lines": "Den Haag HS - Rotterdam Centraal", "rdt_lines_id": "11"}
    record_two = {"rdt_id": "40504", "rdt_lines": "Alkmaar - Den Helder, Alkmaar - Hoorn", "rdt_lines_id": "162,163"}
    