# Transforming and Cleaning IoT Data

In this activity, you’ll combine you skills in data transformation via Python and Pandas methods with those in regular expressions to transform an Internet of Things (IoT) dataset into a DataFrame.

## Instructions

1. Read the `temperature_data.csv` file into a DataFrame, using `sep=","` as the delimiter and `header=None`.

2. Convert the JavaScript Object Notation (JSON) string data to a Python dictionary, and then do the following:

    * Create two lists: one for the keys and one for the values.

    * Iterate through the row data in the DataFrame.

    * Convert each row of JSON string data to a Python dictionary.

    * Use a list comprehension to get the keys from the Python dictionary.

    * Use a list comprehension to get the values from the Python dictionary.

    * Append the keys and values to the lists that you previously created.

3. Create a new DataFrame by using the keys as the columns and the nested list of row values as the data.

4. Use regular expressions and the `extract()` function to do the following:

    * Extract the day of the week from the "timestamp" column, and add it to a new column.

    * Extract the date from the "timestamp" column, and add it to a new column.

    * Extract the time of day from the "timestamp" column, and add it to a new column.

5. Convert the "date" column to the following format: `%Y-%m-%d`, and then convert the date to a `datetime64[ns]` format.

6. Drop the "timestamp" column, and then reorder the columns to get the required DataFrame, as the following image shows.

      ![A screenshot depicts the final IoT temperature DataFrame.](Unsolved/Resources/01-final_transformed_cleaned_DataFrame.png)

      In the preceding image, notice that the columns appear in the following order: "device", "timestamp", "temperature", "pressure", "humidity".

---

Copyright 2022 2U. All Rights Reserved.