# Unit Testing in TrainETL Project

## Overview
Unit testing is a crucial part of the TrainETL project to ensure the reliability and correctness of the code. This document provides an overview of the unit tests implemented within the project.

## List of Unit Tests

### Testcase 1: Load
"""
Test the creation of a database table and the insertion of data from a pandas DataFrame.

Test Case:
1. Reset the database by dropping the "Person" table if it exists.
2. Create a new "Person" table with columns: id, name, birthdate, and age.
3. Create a pandas DataFrame with sample data containing id, name, birthdate, and age.
4. Insert the DataFrame data into the "Person" table using the to_sql method.
5. Verify that the number of rows inserted into the table is equal to the number of rows in the DataFrame (2 rows).

Expected Result:
The function should successfully create the "Person" table and insert 2 rows of data from the DataFrame into the table.
"""

### Testcase 2: Impute Empty Data
"""
Test the imputation of missing end time values in a DataFrame.

Test Case:
1. Create a DataFrame with start_time and end_time columns, where some end_time values are missing.
2. Use the MissingEndDateTransformer to fill in the missing end_time values with the current date and time.
3. Verify that the missing end_time values are correctly filled.

Expected Result:
The missing end_time values should be filled with the current date and time.
"""

### Testcase 3: Duration Calculation
"""
Test the calculation of duration between start_time and end_time.

Test Case:
1. Create a DataFrame with start_time and end_time columns.
2. Use the MissingEndDateTransformer to calculate the duration between start_time and end_time.
3. Verify that the calculated durations are correct.

Expected Result:
The calculated durations should match the expected values.
"""

### Testcase 4: Cleanup Missing Values
"""
Test the cleanup of missing values in a DataFrame.

Test Case:
1. Create a DataFrame with start_time, end_time, and duration columns, where some end_time and duration values are missing.
2. Use the MissingEndDateTransformer to fill in the missing end_time values and calculate the missing durations.
3. Verify that the missing values are correctly filled and calculated.

Expected Result:
The missing end_time values and durations should be correctly filled and calculated.
"""

### Testcase 5: Column Splitter Incorrect Amount Columns
"""
Test the ColumnSplitter with an incorrect number of columns.

Test Case:
1. Create a DataFrame with a single column to be split.
2. Use the ColumnSplitter to split the column into more columns than available.
3. Verify that a ValueError is raised.

Expected Result:
A ValueError should be raised indicating the mismatch in the number of columns.
"""

### Testcase 6: Column Splitter Correct Horizontal Split
"""
Test the ColumnSplitter with a correct horizontal split.

Test Case:
1. Create a DataFrame with a column containing delimited values.
2. Use the ColumnSplitter to split the column into multiple columns.
3. Verify that the columns are correctly split.

Expected Result:
The columns should be correctly split into the specified number of columns.
"""

### Testcase 7: Conversion Datetime to Key
"""
Test the conversion of datetime to a key.

Test Case:
1. Create a DataFrame with a datetime column.
2. Use the DateKeyHandler to convert the datetime values to keys.
3. Verify that the keys are correctly generated.

Expected Result:
The keys should be correctly generated from the datetime values.
"""

### Testcase 8: Column Splitter Correct Vertical Split
"""
Test the ColumnSplitter with a correct vertical split.

Test Case:
1. Create a DataFrame with a column containing delimited values.
2. Use the ColumnSplitter to split the column into multiple rows.
3. Verify that the rows are correctly split.

Expected Result:
The rows should be correctly split into the specified number of rows.
"""

### Testcase 9: String Remover
"""
Test the removal of a substring from specified columns.

Test Case:
1. Create a DataFrame with columns containing a specific substring.
2. Use the StringRemover to remove the substring from the specified columns.
3. Verify that the substring is correctly removed.

Expected Result:
The substring should be correctly removed from the specified columns.
"""

### Testcase 10: SCD1 Add Records
"""
Test the addition of records using SCD Type 1.

Test Case:
1. Create a DataFrame with new records.
2. Use the SCDType1Transformer to add the records to the target table.
3. Verify that the records are correctly added.

Expected Result:
The records should be correctly added to the target table.
"""

### Testcase 11: SCD1 Add and Update
"""
Test the addition and update of records using SCD Type 1.

Test Case:
1. Create a DataFrame with existing and new records.
2. Use the SCDType1Transformer to add and update the records in the target table.
3. Verify that the records are correctly added and updated.

Expected Result:
The records should be correctly added and updated in the target table.
"""

### Testcase 12: SCD1 Add and Update on Multiple Keys
"""
Test the addition and update of records using SCD Type 1 with multiple keys.

Test Case:
1. Create a DataFrame with existing and new records, using multiple keys.
2. Use the SCDType1Transformer to add and update the records in the target table based on multiple keys.
3. Verify that the records are correctly added and updated.

Expected Result:
The records should be correctly added and updated in the target table based on multiple keys.
"""
## Running Unit Tests
To run the unit tests, use the following command:
```bash
astro dev pytest
```

Ensure that all tests pass before deploying any changes to the production environment.

## Conclusion
Unit tests are essential for maintaining the quality and reliability of the TrainETL project. Regularly running and updating these tests will help catch issues early and ensure smooth operation.
