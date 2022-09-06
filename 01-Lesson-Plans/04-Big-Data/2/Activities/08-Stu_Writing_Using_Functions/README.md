# Writing and Using Functions

In this activity, you’ll repurpose the code that created the bigfoot plot by writing a function that re-creates the bigfoot plot.

## Instructions

1. Install Ploty while installing Spark.

2. Create a Spark session.

3. Read in the data.

4. Import the dependencies, including Pandas, Ploty, and the Spark SQL functions that you'll need to handle the year-date conversion.

5. Define your function, adding `df` as a parameter.

6. Write the inside of the function, making sure that it does the following:

    * Creates a new variable to add the "year" column by using the `withColumn()` and `year()` functions.
    * Gets the total number of bigfoot sightings per year.
    * Converts the Spark DataFrame to a Pandas DataFrame.
    * Cleans the Pandas DataFrame and renames the columns.
    * Uses Plotly to plot the number of sightings for each year.
    * Returns the plot after the function is called.

---

Copyright 2022 2U. All Rights Reserved.