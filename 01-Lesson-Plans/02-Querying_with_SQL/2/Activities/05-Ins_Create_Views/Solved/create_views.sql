SELECT s.store_id, SUM(amount) AS Gross
FROM payment AS p
  JOIN rental AS r
  ON (p.rental_id = r.rental_id)
    JOIN inventory AS i
    ON (i.inventory_id = r.inventory_id)
      JOIN store AS s
      ON (s.store_id = i.store_id)
      GROUP BY s.store_id;


-- Create view from query
CREATE VIEW total_sales AS
SELECT s.store_id, SUM(amount) AS Gross
FROM payment AS p
JOIN rental AS r
ON (p.rental_id = r.rental_id)
  JOIN inventory AS i
  ON (i.inventory_id = r.inventory_id)
    JOIN store AS s
    ON (s.store_id = i.store_id)
    GROUP BY s.store_id;


-- Query the table view created
SELECT *
FROM total_sales;

-- Drop view
DROP VIEW total_sales;

-- Get the average price for a movie rental
SELECT f.title, p.amount 
FROM film AS f
  JOIN inventory AS i
  ON (f.film_id = i.film_id)
    JOIN rental AS r
    ON (i.inventory_id = r.inventory_id)
      JOIN payment AS p
      ON (r.rental_id = p.rental_id)
      WHERE p.amount > 0.00;

-- Create a view for the rental price of each movie.
  CREATE VIEW film_rental_price AS 
  SELECT f.title, p.amount 
  FROM film AS f
    JOIN inventory AS i
    ON (f.film_id = i.film_id)
      JOIN rental AS r
      ON (i.inventory_id = r.inventory_id)
        JOIN payment AS p
        ON (r.rental_id = p.rental_id)
        WHERE p.amount > 0.00;

--  Query the table view created
SELECT *
FROM film_rental_price;

-- Get the average payment price for each movie to compare versus the rental rate.
SELECT title, rental_rate, ROUND(AVG(amount),2) AS avg_payment_price 
FROM film_rental_price
GROUP BY title, rental_rate
ORDER BY title;

--  Classify each average movie payment price as greater than or the same as the rental price using a CASE statement.
SELECT title, rental_rate, ROUND(AVG(amount),2) AS avg_payment_price,
	(CASE WHEN AVG(amount) > rental_rate THEN 'Greater than rental rate'
	 ELSE 'The same as the rental price' END) AS "Avg. Payment v. Rental Rate"
FROM film_rental_price
GROUP BY title, rental_rate
ORDER BY "Avg. Payment v. Rental Rate" DESC;

