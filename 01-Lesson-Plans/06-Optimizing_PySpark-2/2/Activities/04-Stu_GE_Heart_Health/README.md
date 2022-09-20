#  Heart Health Great Expectations

In this activity, you'll write Great Expectations tests to validate the data after you read it into a DataFrame and during the transformation process.

### Instructions

1. Upload the `heart_health.csv` dataset into your Google Drive.

2. Create a Spark DataFrame, then create a Great Expectations DataFrame.

3. Create two tests against the original dataset that retrieves the values from the `"success"` fields for the following Expectations:

    * The number of rows is equal to 799.

    * The "State" column only has the following values, `New York', 'Texas', 'California', 'Ohio', 'Washington'`.

4. Write a function that creates a temporary view from the original "heart_health_df" DataFrame, then the transforms the temporary view by retrieving the "State" and "LocationDesc" columns where the state is `'New York'`.

5. Write a conditional expression that does the following: 
    * If the first tests failed, then the pipeline does not transform any data. 

    * If the first test passes, then call the `transform_data()` function you wrote in Step 4 and perform the following tests against the transformed DataFrame: 
        * Test if `New York` is the only value in the "State" column.
        * Test if `Texas`, `California`, `Ohio`, `Washington` values are not present in the "State" column.
        * Test if the row count is between 1 and 500.

    * Add a conditional expression that prints if the transformation passed or failed.

---

Copyright 2022 2U. All Rights Reserved.