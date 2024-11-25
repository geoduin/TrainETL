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

        length = len(splitted_columns.columns)
        print(length)
        if(length != len(new_columns)):
            raise ValueError("Provided columns should equal the amount of horizontally splitted columns.")
        
        df[new_columns] = splitted_columns
        df[new_columns] = df[new_columns].apply(lambda x: x.str.strip())
        return df
    
    def split_column_vertically(self, df: DataFrame, columns: list, delimiter:str):
        for col in columns:
            df[col] = df[col].str.split(delimiter)
            df[col] = df[col].apply(lambda x: x.str.strip())
        df = df.explode(columns)
        return df