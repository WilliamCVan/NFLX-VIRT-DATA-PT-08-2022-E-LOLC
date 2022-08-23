DROP TABLE IF EXISTS products_sold;
DROP TABLE IF EXISTS products_ordered;

CREATE TABLE products_sold (
    product_id SERIAL,
    category VARCHAR,
    name VARCHAR
);

CREATE TABLE products_ordered (
    product_id SERIAL,
    category VARCHAR,
    name VARCHAR
);

INSERT INTO products_sold (category, name)
VALUES
('clothes', 'pants'),
('furniture', 'lamp'),
('books', 'Harry Potter'),
('toys', 'doll');

INSERT INTO products_ordered (category, name)
VALUES
('clothes', 'shoes'),
('furniture', 'chair'),
('books', 'World Atlas'),
('toys', 'squirt gun');