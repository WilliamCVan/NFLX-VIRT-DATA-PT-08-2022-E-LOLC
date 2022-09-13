# Big Data Homework: Amazon Video Game Reviews

For this assignment, you’ll use your knowledge of Spark and of how to write functions to determine the total number of video game reviews from each customer. You'll then filter the original Spark DataFrame to determine the number of 1-, 2-, 3-, 4-, and 5-star video game reviews from each customer.

## Before You Begin

1. Create a new repository, named `Amazon_Video_Game_Reviews`, for this homework assignment.

    **Important:** Don’t add this assignment to an existing repository.

2. Clone the `Amazon_Video_Game_Reviews` repository to your computer.

## Instructions

1. Rename the `Amazon_Video_Game_Reviews_starter_code.ipynb` file to `Amazon_Video_Game_Reviews_Assignment.ipynb`.

2. Read the `https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Video_Games_v1_00.tsv.gz` file into a Spark DataFrame by using the provided code.

3. Import the necessary PySpark SQL functions.

4. Write a function, named `customer_counts()`, that creates a Spark DataFrame and that returns the number of video game reviews from each customer (`customer_id`), in descending order. The function must do the following:

    * Create a column that contains the number of reviews from each "customer_id".
    * Rename the "count" column to "customer_counts".
    * Sort the "customer_counts" column in descending order.

5. Pass the original DataFrame to the `customer_counts()` function. 

6. Filter the original DataFrame to create five new DataFrames, one for each star rating.

7. Get the number of video game reviews from each customer for each filtered DataFrame by passing the filtered DataFrame to the `customer_counts()` function. 

8. Download your `Amazon_Video_Game_Reviews_Assignment.ipynb` file and upload into your "Amazon_Video_Game_Reviews" GitHub repository. 

## Grading Requirements

This assignment will be evaluated against the rubric and assigned a grade according to the following table:

| Grade | Points |
| --- | --- |
| High Pass | 90 or more |
| Pass | 70&ndash;89 |
| Fail | 1&ndash;69 |
| Incomplete | 0 |

## Submission

Before you submit your assignment, check your work against the rubric to make sure that you’re meeting the requirements.

To submit your assignment for grading in Canvas, click Start Assignment, click the Website URL tab, add the URL of your "Amazon_Video_Game_Reviews" GitHub repository, and then click Submit. If you’d like to resubmit your work, communicate with your instructional team to request another opportunity to improve your grade.

---

Copyright 2022 2U. All Rights Reserved.