-- The SUM() window function is use to calculate the total amount paid for the rentals for "customer 184". 
SELECT rental_id, amount, SUM(amount)
OVER () AS total_paid
FROM payment
WHERE customer_id = 184;


-- The results from the window function above would need to be run as two separate queries.
-- 1.
SELECT rental_id, amount 
FROM payment
WHERE customer_id = 184
GROUP BY rental_id, amount
ORDER BY rental_id;

-- 2. 
SELECT SUM(amount)
FROM payment
WHERE customer_id = 184;

-- With the OVER() clause we can order rows within in a partition using the ORDER BY clause.
SELECT rental_id, amount, SUM(amount)
OVER (ORDER BY amount) 
FROM payment
WHERE customer_id = 184;

-- We can get a running total of the rental payments from customer 184.
SELECT rental_id, amount, SUM(amount)
OVER (ORDER BY rental_id) 
FROM payment
WHERE customer_id = 184;

-- We can determine the total movie rental payments for each staff member.
SELECT rental_id, staff_id, amount, SUM(amount)
OVER (PARTITION BY staff_id) 
FROM payment
WHERE customer_id = 184;

--  Rank the movies by length. 
SELECT title, length,
	RANK() OVER (ORDER BY length DESC)
FROM film;

-- Dense rank
SELECT title, length,
	DENSE_RANK() OVER (ORDER BY length DESC)
FROM film;

-- Partition the movie rank by rating
SELECT title,
	length,
	rating,
	RANK() OVER (PARTITION BY rating
    ORDER BY length DESC)
FROM film;

