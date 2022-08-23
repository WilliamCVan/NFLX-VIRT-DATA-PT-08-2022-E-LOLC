-- 1. Select the number of actors by first names in descending order.
SELECT first_name, COUNT(first_name) AS "actor count"
FROM actor
GROUP BY first_name
ORDER BY "actor count" DESC;

-- 2. Select the average duration of movies by rating in ascending order.
SELECT rating, ROUND(AVG(rental_duration),2) AS "avg duration"
FROM film
GROUP BY rating
ORDER BY "avg duration";

-- 3. Select the top ten replacement costs based on the length of the movie in descending order.
SELECT length, ROUND(AVG(replacement_cost)) AS "avg cost"
FROM film
GROUP BY length
ORDER BY "avg cost" DESC
LIMIT 10;

-- Bonus: Using the payment and customer tables, list the total paid by each customer in descending order.
SELECT  c.first_name, c.last_name, SUM(p.amount) AS "Total Amount Paid"
FROM payment AS p
JOIN customer AS c
ON p.customer_id =  c.customer_id
GROUP BY c.first_name, c.last_name
ORDER BY "Total Amount Paid" DESC;
