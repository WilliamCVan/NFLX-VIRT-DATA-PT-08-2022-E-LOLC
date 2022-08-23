# Joining Movies

In this activity, you’ll practice using joins by combining data from various tables.

## Instructions

1. Using `INNER JOIN` and the `customer` and `address` tables, write a PostgreSQL statement to display the first and last names and the street addresses of all the customers.

2. Using `JOIN` and the `customer` and `payment` tables, write a PostgreSQL statement to display the first and last names of each customer who made a payment greater than $10.00.

3. Using `JOIN` and the `actor` and `film` tables, write a PostgreSQL statement to display the first and last names of the actors who starred in ALTER VICTORY.

    **Hint:** You have to join the `actor` and `film_actor` tables and then the `film` and `film_actor` tables.

4. Using `JOIN` and the `customer` and `film` tables, write a PostgreSQL statement to display the nonduplicated first and last names of the customers who rented ALTER VICTORY for `store_id` = 1.

    **Hint:** You have to join the `customer` and `inventory` tables and then the `inventory` and `film` tables.

    **Hint:** To return data without including duplicates, you need to use the `DISTINCT` clause. To learn how to use it, see this [PostgreSQL SELECT DISTINCT tutorial](https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-select-distinct/).

---

Copyright 2022 2U. All Rights Reserved.cd