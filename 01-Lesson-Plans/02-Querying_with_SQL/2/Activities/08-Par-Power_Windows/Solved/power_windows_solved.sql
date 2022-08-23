-- 1. Get a running total of the selling price for the items in the Baby Products category.
SELECT product_name, selling_price, SUM(selling_price)
OVER (ORDER BY product_name)
FROM orders
WHERE category = 'Baby Products';


-- 2. Rank the products from the most expensive to the cheapest. 
SELECT product_name, 
	selling_price,
	RANK() OVER (ORDER BY selling_price DESC) 
FROM orders; 

-- 3. Rank the products by category from the most expensive to the cheapest. 
SELECT product_name, 
	category,
	selling_price,
	RANK() OVER (PARTITION BY category 
				 ORDER BY selling_price DESC) 
FROM orders;

-- Bonus: 
-- Write a query that counts each shipping method and ranks the counts for each category.

SELECT category, shipping_method, COUNT(shipping_method),
	RANK() OVER (PARTITION BY category
		ORDER BY COUNT(shipping_method) DESC)
FROM orders
GROUP BY category, shipping_method;