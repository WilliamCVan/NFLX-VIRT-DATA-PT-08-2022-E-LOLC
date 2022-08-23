# Data Normalization

## First Normal Form (1NF)

* Each field in a table row contains a single value.

* Each row is unique.

## Second Normal Form (2NF)

* The data is already in 1NF.

* A single column is the primary key, which uniquely identifies each row.

## Third Normal Form (3NF)

* The data is already in 2NF.

* No transitive dependencies exist between columns.

  * A **transitive dependency** exists if changing a value in one column might cause a value in another column to change.

  * For example, consider the following columns in the `Vehicle` table: `VIN`, `Customer ID`, and `Model`. If a car (with a particular `VIN`) gets sold to a new customer, the `Customer ID` column will get updated. When the `Customer ID` column changes, the `Model` column also needs updating. A transitive dependency thus exists between the `Model` and `VIN` columns.

---

Copyright 2022 2U. All Rights Reserved.