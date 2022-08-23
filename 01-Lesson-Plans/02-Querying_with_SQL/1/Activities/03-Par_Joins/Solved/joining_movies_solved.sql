-- 1. Retrieve all the first and last names, and the street address, for all the customers.
SELECT  c.first_name, c.last_name, a.address
FROM customer as c
INNER JOIN address as a
ON a.address_id = c.address_id;

-- 2. Retrieve the first and last name for each customer who made a payment over $10.00
SELECT c.first_name, c.last_name, p.amount
FROM customer as c
JOIN payment as p
ON c.customer_id = p.customer_id
WHERE p.amount > 10.00;

-- 3. Retrieve the first and last names of the actors who starred in 'ALTER VICTORY'.
SELECT a.first_name, a.last_name, f.title
FROM actor as a
JOIN film_actor as fa
ON a.actor_id = fa.actor_id
JOIN film as f
ON f.film_id = fa.film_id
WHERE f.title = 'ALTER VICTORY';

-- 4. While avoiding duplicates, retrieve the first and last names of the customers who rented 'ALTER VICTORY' from `store_id` = 1.
SELECT DISTINCT c.first_name, c.last_name, f.title
FROM customer as c
JOIN inventory as i
ON c.store_id = i.store_id
JOIN film as f
ON i.film_id = f.film_id
WHERE f.title = 'ALTER VICTORY'
AND i.store_id = 1;
