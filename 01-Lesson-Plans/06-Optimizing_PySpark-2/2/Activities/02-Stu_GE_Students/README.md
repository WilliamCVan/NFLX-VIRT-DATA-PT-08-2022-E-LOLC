#  Student Testing Data in Great Expectations

In this activity, you'll write basic data quality tests to validate the data after you read it into a DataFrame. 

### Instructions

1. Upload the `students.csv` dataset into your Google Drive.

2. Create a Spark DataFrame, then create a Great Expectations DataFrame.

3. Create a Great Expectations test that checks that there are seven columns.

4. Create a Great Expectations test that checks that there are 1,000 rows.

5. Create a Great Expectations test that checks that the columns match the following ordered list and passes.  

    ```python
    ['student_id',
    'student_name',
    'gender',
    'grade',
    'school_name',
    'reading_score',
    'math_score']
    ```

6. Create a Great Expectations test that checks if the `grade` column only has `9th, 10th, 11th, and 12th` as values.

### Bonus

* Create an Great Expectations test that checks if the `math_score` column datatype is an integer datatype. 

    * **Hint:** Look over the [Spark Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) to assist you. 

---

Copyright 2022 2U. All Rights Reserved.