# Demographic DataFrame Basics

In this activity, you’ll use PySpark DataFrame basics to analyze demographic data. 

## Instructions

1. Install Spark and Java by using the provided code.

2. Start a Spark session.

3. Read in the `demographics.csv` file from the S3 bucket.

4. Print the column names.

5. Print the first 10 rows.

6. Select the "Age", "Height_meter", and "Weight_kg" columns, and then use the `describe()` method to show the summary statistics.

7. Print the schema to observe the types.

8. Rename the "Salary" column to "Salary (1k)", and then show only the new column.

9. Create a new column named "Salary", where each value is the corresponding "Salary (1k)" value × 1000. Show both columns.

---

Copyright 2022 2U. All Rights Reserved.