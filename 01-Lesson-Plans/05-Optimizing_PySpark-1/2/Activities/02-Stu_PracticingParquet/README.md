# Practicing Parquet

In this activity you'll practice storing data in Parquet format and executing queries on Parquet data using Spark.

**Instructions:**

1. Open [Google Colab](https://colab.research.google.com/), and create a new notebook. 

2. Using the starter code provided, start a Spark session in your Colab notebook. 

3. Import the `Austin 311 Public Dataset` to a Spark DataFrame in your Colab notebook.

4. Create a temporary view and write a SparkSQL query that gets a count of each unique name in `description`.
   
    * Record the execution time of the SparkSQL query using the `time.time()` method. 
   
    * **Note:** You may want to run this query twice to eliminate initial load time.

5. Write your Spark DataFrame containing Austin 311 s to parquet format.

6. Read your parquet data into a new Spark DataFrame.

7. Using your new parquet DataFrame, create a new temporary view and write a SparkSQL query that gets a count of each unique name in `description`.

    * Be sure to record the execution time using the new parquet DataFrame temporary view.

8. Compare the runtime of the parquet SparkSQL query versus the traditional Spark DataFrame query. 

---

Copyright 2022 2U. All Rights Reserved.