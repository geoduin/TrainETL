class ColumnSplitter:
    def __init__(self):
        pass

    def split(self, df, delimiter:str, new_column: str, is_vertically:bool = False):
        """
        If the splitted result results into three or more elements
        """
        return df