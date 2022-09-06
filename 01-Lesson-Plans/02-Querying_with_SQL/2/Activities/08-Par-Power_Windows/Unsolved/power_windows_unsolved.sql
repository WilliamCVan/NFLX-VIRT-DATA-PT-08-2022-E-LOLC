-- 1. Get a running total of the selling price for the items in the Baby Products category.

SELECT product_name, sum(selling_price)
OVER (ORDER by product_name)
from orders
where category = 'Baby Products'
ORDER by product_name;

-- 2. Rank the products from the most expensive to the cheapest. 
SELECT product_name, category, selling_price,
	RANK() OVER (ORDER BY selling_price DESC) 
	FROM orders
ORDER BY selling_price DESC;

-- 3. Rank the products by category from the most expensive to the cheapest. 
SELECT product_name, category, selling_price,
	RANK() OVER (Partition by category ORDER BY order_id DESC) 
	FROM orders
ORDER BY selling_price DESC;


-- Bonus: 
-- Write a query that counts each shipping method and ranks the counts for each category.d.
