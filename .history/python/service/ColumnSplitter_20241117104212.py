class ColumnSplitter:
    def __init__(self):
        pass

    def split(self, df, delimiter:str, new_columns: list[str], is_vertically:bool = False):
        """
        If the splitted result results into three or more elements
        The is_vertically variabel is meant to split a dataframe into 
        """
        return df