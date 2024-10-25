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
        file_path = base_dir + "/usr/local/include/test.csv"
        exists = pd.read_csv(file_path)
    except:
        ss = base_dir + "/usr/local/include/test.csv"
        print(ss)
        answer = "Not found"
    assert base_dir == "Found"