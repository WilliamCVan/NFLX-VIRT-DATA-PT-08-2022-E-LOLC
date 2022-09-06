-- 1. What is the average cost to rent a film in the stores?

SELECT AVG(rental_rate) as "Average Rental Rate"
FROM film

SELECt *
FROM film

-- 2. What is the average rental cost of films by rating? On average, what is the cheapest rating of films to rent? Most expensive?
SELECT rating AS "Rating", AVG(rental_rate) as "Average Rental Rate"
FROM film
GROUP BY rating;

-- 3. How much would it cost to replace all the films in the database?
SELECT SUM (replacement_cost) as "Replacement Cost"
FROM film;

-- 4. How much would it cost to replace all the films in each ratings category?
SELECT rating as "Rating", SUM(replacement_cost) as "Replacement Cost"
FROM film
GROUP BY rating;


-- 5. How long is the longest movie in the database? The shortest?
SELECT *
from FILM
ORDER BY length DESC;

SELECT MAX(length), MIN (length)
from film;