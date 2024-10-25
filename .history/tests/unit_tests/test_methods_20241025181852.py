import pandas as pd
import os 
base_dir = os.path.dirname(os.path.realpath(__file__))


def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 4


def test_csv_file():
    answer = "Found"
    try:
        exists = pd.read_csv(base_dir + "test.csv")
    except:
        answer = "Not found"
    assert answer == "Found"