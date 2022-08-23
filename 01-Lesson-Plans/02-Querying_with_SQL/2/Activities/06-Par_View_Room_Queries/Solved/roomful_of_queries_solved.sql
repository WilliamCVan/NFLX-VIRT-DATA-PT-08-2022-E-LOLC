-- 1. 
-- Create the subquery
SELECT title,
(SELECT COUNT(inventory.film_id)
	FROM inventory
	WHERE film.film_id = inventory.film_id ) AS "Number of Copies"
FROM film;

-- Create View
CREATE VIEW title_count AS
SELECT title,
(SELECT COUNT(inventory.film_id)
	FROM inventory
	WHERE film.film_id = inventory.film_id ) AS "Number of Copies"
FROM film;

-- Query the view to the titles with 7 copies
SELECT title, "Number of Copies"
FROM title_count
WHERE "Number of Copies" = 7;

-- 2.
-- Create the query. 
SELECT  f.title, i.store_id, 
	COUNT(i.film_id) AS "Number of Copies"
FROM film as f
JOIN inventory as i
ON f.film_id = i.film_id
GROUP BY f.title, i.store_id
ORDER BY f.title, "Number of Copies";

--  Create the view
CREATE VIEW title_count_by_store AS
SELECT  f.title, i.store_id, 
	COUNT(i.film_id) AS "Number of Copies"
FROM film as f
JOIN inventory as i
ON f.film_id = i.film_id
GROUP BY f.title, i.store_id
ORDER BY f.title, "Number of Copies";


--  Query the view to add a new column, "Status", that contains the note, "Order 2 more copies", 
-- or "Order 1 more copy" based on if the number of copies in the inventory for each store.
--  If there are four or more copies then the "Status" should be "Okay".add the status based on the number of copies. 
SELECT title, store_id,
	(CASE WHEN "Number of Copies" = 2 THEN 'Order 2 more copies'
	 WHEN "Number of Copies" = 3 THEN 'Order 1 more copy'
	 ELSE 'Okay' END) AS "Status"
FROM title_count_by_store;