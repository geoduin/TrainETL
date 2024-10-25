import pandas as pd
import os 
from os import listdir
from os.path import isfile, join

base_dir = os.path.dirname(os.path.realpath(__file__))


def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 4


def test_connection_csv_file():
    """
    This unit test will validate if the connection to the local csv file within the container works. 
    Test passes: 
    - File can be found within the include folder
    - Length of result is 1
    """
    answer = "Found"
    try:
        answer = "Found"
        result = pd.read_csv("include/raw_data/test.csv")

        answer = str(len(result))
    except:
        answer = "Not found"
    assert answer == "1"