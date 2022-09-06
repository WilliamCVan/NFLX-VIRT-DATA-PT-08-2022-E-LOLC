# Retail Transactions

In this activity, you’ll practice using the `groupBy()` and `agg()` function with the Retail Transaction Data dataset. Understanding how these functions work is important, because that’s another step toward being able to transform data into various forms. You’ll use this skill on a regular basis in your careers.

## Instructions

1. Upload the [RetailTransactions_unsolved.ipynb](Activities/04-Stu_RetailTransactions/Unsolved/RetailTransactions_unsovled.ipynb) file into your Google Drive.

2. Read the [retail_transactions.csv](https://2u-data-curriculum-team.s3.amazonaws.com/nflx-data-science-adv/week-4/retail_transactions.csv) file from S3 into PySpark.

3. Convert the "transaction_amount" column from the string type to the `float` type.

4. Find the average "transaction_amount" value by grouping by "customer_id" value.

5. Find the maximum "transaction_amount" value per customer by grouping by "customer_id" value.

6. Find the sum of the "transaction_amount" values per customer by grouping by "customer_id" value.

7. Find the average "transaction_amount" value per "transaction_date" value.

8. Find the sum of "transaction_amount" values per "transaction_date" value.

9. Find the maximum "transaction_amount" value per "transaction_date" value.

---

Copyright 2022 2U. All Rights Reserved.