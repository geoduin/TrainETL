import pandas as pd
import os 
from os import listdir
from os.path import isfile, join

base_dir = os.path.dirname(os.path.realpath(__file__))


def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 4


def test_csv_file():
    answer = "Found"
    try:
        onlyfiles = [f for f in listdir(base_dir) if isfile(join(base_dir, f))]
        answer = onlyfiles
        rr = pd.read_csv("./test.csv")
    except:
        answer = "Not found"
    assert answer == "Found"