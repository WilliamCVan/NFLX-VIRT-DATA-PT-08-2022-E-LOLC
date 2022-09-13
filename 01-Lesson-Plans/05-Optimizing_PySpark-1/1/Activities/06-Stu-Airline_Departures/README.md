# Airline Departures

In this activity, you’ll use Spark SQL to extract data from an external source, define the schema for the data, create a temporary table, and determine which airport has the most departures.

## Instructions

1. Using the URLs that the notebook provides, examine both datasets. Then define a `StructType` schema object for both data structures.

    * For each column in the data structure, provide a `StructField` object in the list of fields. Be sure to include whether the data is `StringType()` or `IntegerType()`.

    * For reference, note the following example `StructType` schema object for an employee schema:

      ```python
      empSchema= StructType(  
      [StructField("Age",  IntegerType(), True),
      StructField("Department",  StringType(), True),
      StructField("RelationshipSatisfaction",  IntegerType(), True),
      StructField("StandardHours",  IntegerType(), True)
      ])
      ```

2. Using your defined schema objects and the provided code, import the data structures into your Spark session.

3. Create a temporary view for each data structure. 

4. Use Spark SQL to answer the following question: 

    * Which airport, along with its city and state, has the most departures?
 
      **Hint:** You need to join data across views.

## Reference

Data sources:

  *   [airport codes](https://raw.githubusercontent.com/databricks/LearningSparkV2/master/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt)

  * [departures and delays](https://raw.githubusercontent.com/databricks/LearningSparkV2/master/databricks-datasets/learning-spark-v2/flights/departuredelays.csv)

---

Copyright 2022 2U. All Rights Reserved.