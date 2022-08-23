-- Use an inner JOIN to display the first and last names, as well as the address, of each staff member
SELECT staff.first_name, staff.last_name, address.address
FROM staff
INNER JOIN address ON address.address_id = staff.address_id;

-- Use a LEFT JOIN to get the first and last names, as well as the address, of each staff member.
SELECT staff.first_name, staff.last_name, address.address
FROM staff
LEFT JOIN address ON address.address_id = staff.address_id;

-- Use a LEFT JOIN to get the addresses for the stores. 
SELECT store.address_id, 
	address.address, 
	address.district,
	address.postal_code
FROM store
LEFT JOIN address ON store.address_id = address.address_id;

--  Use a FULL OUTER JOIN to get the all the addresses for the stores.
SELECT store.address_id, 
	address.address, 
	address.district,
	address.postal_code
FROM address
FULL OUTER JOIN store ON address.address_id = store.address_id;

-- Use a CROSS JOIN to join two tables.
SELECT store.address_id, 
	address.address, 
	address.district,
	address.postal_code
FROM address
CROSS JOIN store;