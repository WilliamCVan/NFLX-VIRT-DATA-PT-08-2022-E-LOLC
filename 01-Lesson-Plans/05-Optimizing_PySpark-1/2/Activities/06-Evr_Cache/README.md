# Cache is King!

In this activity you'll import three datasets, practice creating partitions, filter the data, and create temporary views of the three datasets. Then, you'll write queries to join three datasets, and determine the execution times of the queries. Finally, you'll cache your data and compare query execution times with the cached 

**Instructions:**

1. Open [Google Colab](https://colab.research.google.com/) and create a new notebook. 

2. Using the starter code provided, start a Spark session in your Colab notebook. 

    * Code has been included to set the number of shuffle partitions.

3.  Upload and import the following datasets into your Spark session:

    * [DelayedFlights.csv](https://2u-data-curriculum-team.s3.amazonaws.com/nflx-data-science-adv/week-5/DelayedFlights.csv)
    * [500 City latitude and longitude data](https://2u-data-curriculum-team.s3.amazonaws.com/nflx-data-science-adv/week-5/cities500.txt)
    * [airportCodes.csv](https://2u-data-curriculum-team.s3.amazonaws.com/nflx-data-science-adv/week-5/airportCodes.csv)

    * **NOTE:** The `DelayedFlights.csv` file is comma-delimited, while the `cities500.txt` file is tab-delimited. Be sure to declare the appropriate separator when importing each dataset. 

4. Filter the airport codes Spark DataFrame to only contain those whose `country` equals `USA`.

5. Filter the 500 city latitude and longitude DataFrame to only contain the `name`,`latitude`,`longitude`,`admin1_code` fields and rows whose `country_code` equals `US`.

6. Create temporary views for all three Spark DataFrames. Name the temporary views as follows:
    
    * The delayed flight temporary view as, `delayed`.
    * The latitude and longitude temporary view as, `lookup_geo`.
    * The airport codes temporary view as, `lookup_city`.

7. Modify the provided SparkSQL query that was used in the instructor demonstration to add `origin_latitude`, `origin_longitude`, `dest_latitude` and `dest_longitude` fields from the joined temporary views.
   
    * **NOTE**:  The two lookup tables do not have matching columns, so you must be mindful of what names are  used when joining both views together.   

8. Use the same SQL query as in Step 7, but this time add a "Broadcast" hint for either the `lookup_geo` or `lookup_city` temporary views and run your query again. 

    * Was there any change in the runtime of the query? Why or why not?

9. Use the same SQL query as in Step 8, but this time use an aggregate function (i.e. `avg()`, `sum()`, etc.) on some of the fields from the `delayed` flights temporary view. 
   
    * Add enough aggregate functions to increase the runtime of your SparkSQL query. 

10. Use SparkSQL to cache the `delayed` temporary view, since it is the largest table. 

11. Verify that you successfully cached your table using the `spark.catalog.isCached()` method.

12. Once again, rerun your aggregating SparkSQL query from Step 9 and note the runtime.

    * Did caching decrease the runtime of your SparkSQL query? Why or why not?

13. Cache one of the lookup tables.

14. Run the query from Step 8 again. You should continue to see improvement.

15. Uncache anything that you have previously cached.

16. Verify that nothing is cached using the `spark.catalog.isCached()` method.


---

Copyright 2022 2U. All Rights Reserved.