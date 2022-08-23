-- 1. List the names and ID numbers of cities that are in the following list: `Qalyub`, `Qinhuangdao`, `Qomsheh`, `Quilmes`.
SELECT city, city_id
FROM city
WHERE city IN ('Qalyub', 'Qinhuangdao', 'Qomsheh', 'Quilmes');


-- 2. Display the districts in the above list of cities.
SELECT district
FROM address
WHERE city_id IN
(
  SELECT city_id
  FROM city
  WHERE city IN ('Qalyub', 'Qinhuangdao', 'Qomsheh', 'Quilmes')
);


-- Bonus: Using subqueries, find the first and last names of customers who reside in cities that begin with the letter *Q*.
SELECT first_name, last_name
FROM customer cus
WHERE address_id IN
(
  SELECT address_id
  FROM address a
  WHERE city_id IN
  (
    SELECT city_id
    FROM city
    WHERE city LIKE 'Q%'
  )
);
