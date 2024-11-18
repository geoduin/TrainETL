class ColumnSplitter:
    def __init__(self):
        pass

    def split_columns(self, df, delimiter:str, new_columns: list[str]):
        """
        Variables accepted:
        - df is a DataFrame
        - delimiter is a string
        - new_columns is a list of string

        Mandatory:
        - The new_column length should contain the same amount of elements as the splitted result.
        - Delimiter is mandatory.
        """
        return df