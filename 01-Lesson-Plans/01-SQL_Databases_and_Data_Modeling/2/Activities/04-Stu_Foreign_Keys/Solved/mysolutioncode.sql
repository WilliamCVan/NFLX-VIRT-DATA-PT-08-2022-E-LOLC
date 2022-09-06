CREATE TABLE customer (
id SERIAL primary key,
first_name varchar(30) not NULL,
last_name varchar(30) not NULL
);

INSERT INTO customer (first_name, last_name)
VALUES
('Test','TestSample100'),
('John', 'Smith'),
('Jane', 'Doe');

CREATE TABLE customer_email (
	id SERIAL primary key,
	customer_email varchar(30) not NULL,
	customer_id INTEGER NOT NULL,
FOREIGN KEY (customer_id) REFERENCES customer (id)
)

INSERT INTO customer_email (customer_id, customer_email)
VALUES
(1,'example@gmail.com'),
(2,'joesmith@gmail.com'),
(3, 'janedoe@gmail.com');

CREATE TABLE customer_phone (
	id SERIAL primary key,
	customer_phone VARCHAR(20) NOT NULL,
	customer_id INTEGER NOT NULL,
FOREIGN KEY (customer_id)REFERENCES customer (id));

INSERT into customer_phone(customer_id, customer_phone)
VALUES
(1,0000000000),
(2,1111111111),
(3,2222222222);

SELECT customer.first_name, customer.last_name, email.customer_email, phone.customer_phone
FROM customer
JOIN customer_email AS email
ON customer.id = email.customer_id
JOIN customer_phone AS phone
ON customer.id = phone.customer_id;