from pandas import DataFrame

class ColumnSplitter:
    def __init__(self):
        pass

    def split_columns(self, df: DataFrame, column:str, delimiter:str, new_columns: list[str]):
        """
        Variables accepted:
        - df is a DataFrame
        - delimiter is a string
        - new_columns is a list of string

        Mandatory:
        - The new_column length should contain the same amount of elements as the splitted result.
        - Delimiter is mandatory.
        """
        splitted_columns = df[column].str.split(delimiter)

        len(splitted_columns.values)
        return df
    
    def split_column_vertically(self, df, delimiter:str, new_column: str):
        raise NotImplementedError("Not implemented dumbass")