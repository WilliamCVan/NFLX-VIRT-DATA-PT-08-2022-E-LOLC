# Pet Normalizer

In this activity, you’ll practice data normalization skills by using the provided data.

## Instructions

1. In pgAdmin, create a database named `pets_db`.

2. Using Excel, get the data into 1NF.

3. Using the normalized CSV file, create the following tables with continued normalization practices:

    * A table for owners that for each owner, contains an ID and the owner’s name

    * A table for pet names that for each pet, contains two IDs, the pet’s name, and the pet's type

4. Using the CSV file as a guide, insert the data into the appropriate tables.

## Hint

Be sure that each table has a unique primary key.

## Bonus

1. Create a `service` table that contains the types of offered services.

2. Create a `pet_names_updated` table that for each pet, contains an ID that connects to the `service` table.

3. Join all three tables.

## Reference

Data generated by Trilogy Education Services, a 2U, Inc. brand, and is intended for educational purposes only.

---

Copyright 2022 2U. All Rights Reserved.