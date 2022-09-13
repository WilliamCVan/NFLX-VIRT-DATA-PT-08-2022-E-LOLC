# PySpark Homework: Home Sales

## Background

In this assignment, you will use your knowledge of SparkSQL to determine key metrics about a home sales data, then you'll use Spark to create temporary views, partition the data, cache and uncache a temporary table, and verify that the table has been uncached. 

### Before You Begin

1. Create a new repository, named "Home_Sales", for this homework assignment.

    **Important:** Don’t add this assignment to an existing repository.

2. Clone the "Home_Sales" repository to your computer.

### Instructions

1. Rename the `Home_Sales_starter_code.ipynb` file as `Home_Sales_Assignment.ipynb`.

2. Import the necessary PySpark SQL functions for this assignment.

3. Read in the [home_sales_revised.csv](https://2u-data-curriculum-team.s3.amazonaws.com/nflx-data-science-adv/week-5/Assignment/home_sales_revised.csv) in the starter code into a Spark DataFrame.

4. Create a temporary table called `home_sales`.

5. Answer the following questions using SparkSQL:

    * What is the average price for a four bedroom house sold in 2019 rounded to two decimal places?
    
    * What is the average price of a home with 3 bedrooms and 3 bathrooms for each year rounded to two decimal places?

    * What is the average price of a home with 3 bedrooms, 3 bathrooms, and less than 2,000 square feet for each year rounded to two decimal places?

    *  What is the "view" rating for the average price of a home rounded to two decimal places where the homes are less than $350,000?  Although this is a small determine the run time for this query.


6. Cache your temporary table `home_sales`.

7. Check if your temporary table is cached.

8.  Using the cached data, run the query that filters out the view ratings with average price of less than $350,000. And, determine the runtime and compare it to uncached runtime.

9. Partition the home sales dataset by the date_built field and the formatted parquet data is read.

10. Create a temporary table for the parquet data.

11. Run the query that filters out the view ratings with average price of less than $350,000. And, determine the runtime and compare it to uncached runtime.

13. Uncache the `home_sales` temporary table.

13. Verify the `home_sales` temporary table is uncached using PySpark.

14. Download your `Home_Sales_Assignment.ipynb` file and upload it into your "Home_Sales" GitHub repository. 

## Grading Requirements

This assignment will be evaluated against the rubric and assigned a grade according to the following table:

| Grade | Points |
| --- | --- |
| High Pass | 90 or more |
| Pass | 70&ndash;89 |
| Fail | 1&ndash;69 |
| Incomplete | 0 |

## Submission

Before you submit your assignment make sure to check your work against the rubric to ensure you are meeting the requirements. 

To submit your assignment for grading in Canvas, click Start Assignment, click the Website URL tab, click the Website URL tab, add the URL for the solution to "Home_Sales" on your GitHub repository, and then click Submit.  If you would like to resubmit your work, talk to your instructional team to request another opportunity to improve your grade.


---

Copyright 2022 2U. All Rights Reserved.