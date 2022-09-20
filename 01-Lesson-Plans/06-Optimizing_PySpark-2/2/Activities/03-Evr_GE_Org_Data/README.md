# Great Expectations Quality Test Placement

In this activity, you'll write Great Expectation tests to validate the data after you read it into a DataFrame and for the transformation process. 

## Instructions

1. Upload the `orgs.csv` dataset into your Google Drive.

2. Create a Spark DataFrame, then create a Great Expectations DataFrame.

3. Retrieve the values from the "success" fields for the column and row count, and matching columns.

4. Write a function that creates a temporary view, `orgs` from the original Spark DataFrame. Create a new DataFrame from the temporary view by retrieving only the rows that contain the value, `School Organization`, from the "TYPE" column using Spark SQL. Then, convert the transformed DataFrame into a Great Expectations DataFrame.

5. Write a conditional expression that does the following: 
    * If the first tests failed, then the pipeline will not transform any data. 

    * If the first tests passed, then call the `transform_data()` function and perform the following tests against the transformed DataFrame: 
      * Test if the columns match an ordered list.
      * Test if the values from the "TYPE" column are `School Organization`

6. Write another conditional expression that prints out if the data transformation passed or failed.

---

Copyright 2022 2U. All Rights Reserved.