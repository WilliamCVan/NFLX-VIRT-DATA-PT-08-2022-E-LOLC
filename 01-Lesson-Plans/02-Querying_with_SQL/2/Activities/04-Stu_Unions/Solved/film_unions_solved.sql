
-- 1. Using `UNION`, write a PostgreSQL statement to query the number of rows in tables `city` and `country`.

SELECT COUNT(*)
FROM city
UNION
SELECT COUNT(*)
FROM country;


-- 2. Use `UNION` to display from the tables `customer` and `customer_list` the ID of all customers who live in the city of London. 
-- Determine whether both tables contain the same customers by using `UNION ALL`.

SELECT customer_id
FROM customer
WHERE address_id IN
(
  SELECT address_id
  FROM address
  WHERE city_id IN
  (
    SELECT city_id
    FROM city
    WHERE city = 'London'
  )
)
UNION ALL
SELECT id
FROM customer_list
WHERE city = 'London';


