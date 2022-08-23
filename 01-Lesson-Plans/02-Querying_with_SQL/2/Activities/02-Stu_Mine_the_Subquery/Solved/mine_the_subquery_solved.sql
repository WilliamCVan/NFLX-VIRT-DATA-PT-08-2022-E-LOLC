-- 1. Using subqueries, identify all actors who appear in the film _Alter Victory_ in the `pagila` database.

SELECT first_name, last_name
FROM actor
WHERE actor_id IN
(
  SELECT actor_id
  FROM film_actor
  WHERE film_id IN
  (
    SELECT film_id
    FROM film
    WHERE title = 'ALTER VICTORY'
  )
);

--  You can check the results above with the following JOIN query. 
SELECT a.first_name, a.last_name
FROM actor as a
JOIN film_actor as fa
ON a.actor_id = fa.actor_id
JOIN film as f
ON fa.film_id = f.film_id
WHERE f.title = 'ALTER VICTORY';


-- 2. Using subqueries, display the titles of films that were rented by the customer, PEGGY MYERS.

SELECT title
FROM film
WHERE film_id
IN (
  SELECT film_id
    FROM inventory
    WHERE inventory_id
    IN (
        SELECT inventory_id
        FROM rental
        WHERE customer_id
        IN (
              SELECT customer_id
              FROM customer
              WHERE first_name = 'PEGGY' AND last_name = 'MYERS'
            )
        )
  );

-- You can check the results above with the following JOIN query.
SELECT  f.title
FROM customer as c
JOIN rental as r
ON c.customer_id = r.customer_id
JOIN inventory as i
ON r.inventory_id = i.inventory_id
JOIN film as f
ON i.film_id = f.film_id
WHERE c.first_name = 'PEGGY' AND c.last_name = 'MYERS'
ORDER BY f.title;
