-- JOIN the two tables 
SELECT po.category, 
	po.name, 
	ps.category, 
	ps.name
FROM products_sold as ps
JOIN products_ordered as po
ON ps.category = po.category;

-- Union
SELECT category, name
FROM products_sold
UNION
SELECT category, name
FROM products_ordered;


-- Two separte queries for toys and games
SELECT toy_id AS id, type
FROM toys;

SELECT game_id AS id, type
FROM games;

-- Union of toys and game types
SELECT toy_id AS id, type
FROM toys

UNION

SELECT game_id AS id, type
FROM games;

-- Include duplicate rows
SELECT toy_id AS id, type
FROM toys

UNION ALL

SELECT game_id AS id, type
FROM games;
