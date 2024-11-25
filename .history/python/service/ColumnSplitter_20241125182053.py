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
        splitted_columns = df[column].str.split(delimiter, expand=True)

        length = len(splitted_columns.values)
        if(length != len(new_columns)):
            raise ValueError("Provided columns should equal the amount of horizontally splitted columns.")
        
        df[new_columns] = splitted_columns
        return df
    
    def split_column_vertically(self, df, delimiter:str, new_column: str):
        raise NotImplementedError("Not implemented dumbass")