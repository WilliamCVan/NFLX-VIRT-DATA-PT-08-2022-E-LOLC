# Power Windows

In this activity, you’ll work with a partner to practice writing queries that use window functions.

## Instructions

1. Write a query to get a running total of the selling prices of the items in the Baby Products category.

2. Write a query that ranks the products from the most to the least expensive.

3. Modify the previous query to rank the products by category from the most to the least expensive.

## Bonus

Write a query that counts the number of times that each shipping method was used and ranks the counts for each category.

* **Hint:** You need to use `PARTITION BY` and `ORDER BY` in the `RANK` window function.

* The results should appear as follows: 

  | | category<br>character varying (40) | shipping_method<br>character varying (10) | count<br>bigint | rank<br>bigint |
  |----|----|----|----| ---|
  | 1 | Arts, Crafts & Sewing | Prime | 18 | 1 |
  | 2 | Arts, Crafts & Sewing | 2-day | 6 | 2 |
  | 3 | Arts, Crafts & Sewing | Standard | 4 | 3 |
  | 4 | Automotive | 2-day | 1 | 1 |
  | 5 | Baby Products | Prime | 15 | 1 |
  | 6 | Baby Products | Standard | 4 | 2 |
  | 7 | Baby Products | 2-day | 2 | 3 |
  | 8 | Beauty & Personal Care | Prime | 2 | 1 |
  | 9 | Clothing, Shoes & Jewelry | Prime | 43 | 1 |
  | 10 | Clothing, Shoes & Jewelry | 2-day | 24 | 2 |
  | 11 | Clothing, Shoes & Jewelry | Standard | 21 | 3 |

---

Copyright 2022 2U. All Rights Reserved.