from pandas import DataFrame

class StringRemover:

    def remove(self, dataframe: DataFrame, string_word: str, columns:list, relocate_removed=False, new_column:str="_moved"):
        """
        This method will remove a piece of a string value and returns the dataframe without.
        """