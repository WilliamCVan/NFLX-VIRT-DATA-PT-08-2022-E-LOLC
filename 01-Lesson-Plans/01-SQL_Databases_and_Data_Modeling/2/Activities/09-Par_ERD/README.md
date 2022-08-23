# Designing an ERD, Part 2

In this activity, you and your partner will continue designing an ERD for the gym by transitioning the logical ERD that you already created to a physical ERD.

## Instructions

1. Return to [QuickDBD](https://app.quickdatabasediagrams.com/#/), and then using the provided starter code, transition your logical ERD to a physical ERD by creating the relationships between tables.

2. When you feel satisfied with your ERD, write a corresponding schema file that contains your `CREATE TABLE` statements.

3. In pgAdmin, connect to your server, and then create a new database named `gym`. Then open a query tool.

4. Paste the code from your schema file into pgAdmin, and then run the code.

## Hints

* To map the data relationships, you need to add foreign keys to your tables.

* You add a foreign key to a table by using the `FK` acronym followed by the relationship. Here’s an example: `OrderID INT FK >- Order.OrderID`.

* Remember to document the relationships between entities by using the correct symbols. The following table lists the allowed symbols and the relationships that they designate:

  | Symbols | Relationships |
  | --- | --- |
  | - | One to one |
  | -< | One to many |
  | <- | Many to one |
  | >-< | Many to many |
  | -0 | One to zero, or one to one |
  | 0- | Zero to one, or one to one |
  | 0-0 | Zero to zero, zero to one, one to zero, or one to one |
  | -0< | One to zero, or one to many |
  | >0- | Zero to one, or many to one |

* Keep the following in mind:

  * Each member belongs to only one gym.

  * Each trainer works for only one gym, but each gym has multiple trainers.

  * Each member must have a single trainer, but each trainer can train multiple members.

  * Each member has one credit card on file.

* Once you’ve created tables in pgAdmin, you can check the table creation by using the following syntax:

  ```sql
  SELECT * FROM members;
  ```

---

Copyright 2022 2U. All Rights Reserved.
