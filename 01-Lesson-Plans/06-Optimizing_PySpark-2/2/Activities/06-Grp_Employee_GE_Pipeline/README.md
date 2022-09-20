# Employee Great Expectations Pipeline

In this activity, you'll write Great Expectation tests to validate the data after you read it into a DataFrame, and during the transformation process.

### Instructions

1. Using the `attrition.csv` file, create a new Great Expectations dataset.

2. Write a test that checks if the columns have null values. If a column contains null values print out the column name. 

3. Create a function called `clean_data()` that does the following:

    * Creates a temporary view called `employees` from the Spark DataFrame.
    * Uses Spark SQL to select all the columns where:
        * Any row that has a null value.
        * The values in the "attrition" column are "Yes".
        * The values in the "StandardHours" column are less than 80.
        * The values in the "Age" column are less than 18.
    * Creates a new temporary view, `removed_employees`, and a parquet file, `removed_employees_parquet`, to store the data that meets the above criteria.

4. Retrieve the "success" fields for the columns in the Great Expectations DataFrame that meet the data quality criteria in Step 3. If a column doesn't meet the criteria then call the `clean_data()` function. 

5. Check if the files that are created by the `clean_data()` function contain the correct data. 

    * **Hint:** There should be 247 rows in the `removed_employees_parquet` file. 

6. Create a function called `transform_data()` that creates a new DataFrame from the `employees` temporary view that excludes the data in the `removed_employees` temporary view, has the following columns from the `employees` temporary view:
    * "id", "Attrition", "BusinessTravel", "DailyRate","Department", "JobRole", "HourlyRate", "MonthlyIncome", "MonthlyRate", "OverTime", "PercentSalaryHike",  "PerformanceRating", and "StandardHours". 
    * And, the new DataFrame has three new columns: 
        * A "Created_At" column that contains a UNIX timestamp when the DataFrame was created.
        * An "Updated_At" column that contains a UNIX timestamp when the DataFrame was updated.
        * A "Source" column that contains the value "From Client".

5. Call the the `transform_data()` function and save the transformed DataFrame as a Great Expectations DataFrame. Then write data quality tests that retrieve the "success" fields for the following:
    * There are 16 columns in the DataFrame.
    * There are no null values in the "Created_At", "Updated_At", and "Source" columns.
    * The "Attrition" column values are "No". 
    * The "StandardHours" column values are between 80 and the maximum value. 
    * If all the test pass then then write the transformed data to a parquet file named, `employee_parquet`. 

6. Check if the parquet file that is created by the `transform_data()` function contain the correct data. 

    * **Hint:** There should be 1,203 rows in the `employee_parquet` file. 

---

Copyright 2022 2U. All Rights Reserved.