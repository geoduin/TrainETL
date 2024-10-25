import pandas as pd
def inc(x):
    return x + 1


def test_answer():
    assert inc(3) == 5


def test_csv_file():
    exists = pd.read_csv("../../include/test.csv")

    assert len(exists) == 0