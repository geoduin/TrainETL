from pandas import DataFrame

class StringRemover:

    def remove(self, dataframe: DataFrame, string_word: str, columns:list[str], relocate_removed=False, new_column:str="_moved"):
        """
        This method will remove a piece of a string value and returns the dataframe without.
        """
        if(len(columns) == 0):
            raise ValueError("Must contain at least one element")
        
        for cols in columns:
            dataframe[cols] = dataframe[cols].str.replace(string_word, "").str.strip()

        return dataframe