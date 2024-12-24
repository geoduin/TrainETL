import pandas as pd
import os 
from unittest.mock import patch
import pytest
from datetime import datetime
from python import MissingEndDateTransformer, ColumnSplitter, DateKeyHandler, StringRemover, SCDType1Transformer, SnowflakeHandler
base_dir = os.path.dirname(os.path.realpath(__file__))

# Test case impute empty data.
connection_string = os.environ.get("SNOWFLAKE_CONNECTION")
snowflake_handler = SnowflakeHandler(connection_string)

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown():
    # Setup: Create test tables and insert initial data
    connection = snowflake_handler.get_connection()
    snowflake_handler.run_raw_query("CREATE TABLE IF NOT EXISTS dim_test (id INT, name STRING, age INT, city STRING);")
    
    yield  # This is where the testing happens

    # Teardown: Drop test tables and close connection
    snowflake_handler.run_raw_query("DROP TABLE IF EXISTS dim_test")
    connection.close()

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

def test_column_splitter_incorrect_amount_columns():
    splitter = ColumnSplitter()
    columns = ["BeginStation"]
    record_one = {"rdt_id": "40500", "rdt_lines": "Den Haag HS - Rotterdam Centraal", "rdt_lines_id": "11"}
    record_two = {"rdt_id": "40504", "rdt_lines": "Alkmaar - Den Helder", "rdt_lines_id": "162,163"}

    dff = pd.DataFrame([record_one, record_two])
    with pytest.raises(ValueError, match="Provided columns should equal the amount of horizontally splitted columns."):
        splitter.split_columns(dff, "rdt_lines", "-", columns)

def test_column_splitter_correct_horizontal_split():
    splitter = ColumnSplitter()
    columns = ["BeginStation", "EndStation"]

    
    record_one = {"rdt_id": "40500", "rdt_lines": "Den Haag HS - Rotterdam Centraal", "rdt_lines_id": "11"}
    record_two = {"rdt_id": "40504", "rdt_lines": "Alkmaar - Den Helder", "rdt_lines_id": "162,163"}

    dff = pd.DataFrame([record_one, record_two]) 
    result: pd.DataFrame = splitter.split_columns(dff, "rdt_lines", "-", columns)

    assert len(result.columns) == 5
    assert result.iloc[0].BeginStation == "Den Haag HS"
    assert result.iloc[0].EndStation == "Rotterdam Centraal"

def test_convertion_datetime_to_key():
    datekeyhandler = DateKeyHandler()

    data = [{"id": 1, "content": "Hello world", "date": datetime(2024, 4, 12, 15, 37, 20)}]
    df = pd.DataFrame(data)
    df["datekey"] = df.apply(lambda row: datekeyhandler.convert_datetime_to_key(row["date"]), axis=1)
    datekey =  df.iloc[0]["datekey"]
    assert datekey == 202404121537

def test_column_splitter_correct_vertical_split():
    splitter = ColumnSplitter()
    columns = ["rdt_lines", "rdt_lines_id"]
    record_one = {"rdt_id": "40500", "rdt_lines": "Den Haag HS - Rotterdam Centraal", "rdt_lines_id": "11"}
    record_two = {"rdt_id": "40504", "rdt_lines": "Alkmaar - Den Helder, Alkmaar - Hoorn", "rdt_lines_id": "162,163"}
    dff = pd.DataFrame([record_one, record_two]) 

    result = splitter.split_column_vertically(dff, columns, ",")
    first_element = result.iloc[1]
    assert len(result) == 3
    assert first_element.rdt_lines == "Alkmaar - Den Helder"
    assert int(first_element.rdt_lines_id) == 162

def test_string_remover():
    string_remover = StringRemover()
    record_one = {"BeginStation": "Den Haag HS (HSL)", "EndStation": "Rotterdam Centraal (HSL)"}
    record_two = {"BeginStation": "Den Haag HS (HSL)", "EndStation": "Rotterdam Centraal"}
    dff = pd.DataFrame([record_one, record_two]) 
    result = string_remover.remove(dff, "(HSL)", ["BeginStation", "EndStation"])

    assert result.iloc[0].BeginStation == "Den Haag HS"
    assert result.iloc[1].BeginStation == "Den Haag HS"
    assert result.iloc[0].EndStation == "Rotterdam Centraal"

def test_scd1_add_records():
    connection_string = "snowflake://Geoduin:Pass4Word@VPCUYOS-GU56680/TreinDatabase/PUBLIC?warehouse=TRAINWAREHOUSE&role=ACCOUNTADMIN"
    snowflake_handler = SnowflakeHandler(connection_string)
    connection = snowflake_handler.get_connection()
    SCD1 = SCDType1Transformer(snowflake_handler)

    # Create test tables
    snowflake_handler.run_raw_query("CREATE TABLE IF NOT EXISTS dim_test (id INT, name STRING, age INT, city STRING);")

    record_one = {"id": 1, "name": "John", "age": 25, "city": "Rotterdam"}
    record_two = {"id": 2, "name": "Alice", "age": 30, "city": "Amsterdam"}
    record_three = {"id": 3, "name": "Alice", "age": 30, "city": "Utrecht"}
    dff = pd.DataFrame([record_one, record_two, record_three]) 

    # Insert records into temp_table 
    dff.to_sql("temp_test", connection, if_exists="replace", index=False)
    # Act
    SCD1.merge_data(dff, "temp_test", "dim_test")

    # Assert
    result = pd.read_sql("SELECT * FROM dim_test", connection)

    assert len(result) == 3
    assert result.iloc[0].city == "Rotterdam"
    assert result.iloc[1].city == "Amsterdam"
    assert result.iloc[2].city == "Utrecht"

    # Clean up
    snowflake_handler.run_raw_query("DROP TABLE IF EXISTS dim_test")

def test_scd1_add_and_update():
    connection_string = os.environ.get("SNOWFLAKE_CONNECTION")
    snowflake_handler = SnowflakeHandler(connection_string)
    connection = snowflake_handler.get_connection()
    SCD1 = SCDType1Transformer(snowflake_handler)

    # Create test tables
    snowflake_handler.run_raw_query("CREATE TABLE IF NOT EXISTS dim_test (id INT, name STRING, age INT, city STRING);")

    record_one = {"id": 1, "name": "John", "age": 25, "city": "Rotterdam"}
    record_two = {"id": 2, "name": "Alice", "age": 30, "city": "Amsterdam"}
    record_three = {"id": 3, "name": "Alice", "age": 30, "city": "Utrecht"}
    record_four = {"id": 1, "name": "Alice", "age": 26, "city": "Den-Haag"}
    dff = pd.DataFrame([record_one, record_two, record_three, record_four]) 

    # Insert records into temp_table 
    dff.to_sql("temp_test", connection, if_exists="replace", index=False)
    # Act
    SCD1.merge_data(dff, "temp_test", "dim_test")

    # Assert
    result = pd.read_sql("SELECT * FROM dim_test", connection)

    assert len(result) == 3
    assert result.iloc[0].city == "Den-Haag"
    assert result.iloc[1].city == "Amsterdam"
    assert result.iloc[2].city == "Utrecht"
    
    # Clean up
    snowflake_handler.run_raw_query("DROP TABLE IF EXISTS dim_test")