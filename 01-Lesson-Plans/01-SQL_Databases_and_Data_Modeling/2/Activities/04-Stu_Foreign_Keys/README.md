# Foreign Keys

In this activity, you’ll create three tables and populate two of them with foreign keys that reference existing data.

## Instructions

1. Create a `customer` table that contains customer first names and customer last names.

2. Create a `customer_email` table with a foreign key that references a field in the original `customer` table.

3. Populate the `customer_email` table with emails.

4. Create a `customer_phone` table with a foreign key that references a field in the original `customer` table.

5. Populate the `customer_phone` table with phone numbers.

6. To test your understanding of foreign keys, write a query that inserts data into the `customer_phone` table by using a reference ID that doesn’t exist in the `customer` table.

7. Join all three tables.

## Hints

* Make sure that each table has a primary key that increments with each new row of data.

* Think about how you can select a column in a table and use that column as a reference to insert data into the table.

---

Copyright 2022 2U. All Rights Reserved.