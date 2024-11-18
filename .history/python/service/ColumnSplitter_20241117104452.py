class ColumnSplitter:
    def __init__(self):
        pass

    def split(self, df, delimiter:str, new_columns: list[str], is_vertically:bool = False):
        """
        If the splitted result results into three or more elements
        The is_vertically variabel is meant to split a dataframe either horizontally into its own column.
        - If the variable is True, these variables will be put into its own record.
        - If False, into its own column.

        Mandatory:
        - If is_vertically is False, the new_column length should contain the same amount of elements as the splitted result.
        """
        return df