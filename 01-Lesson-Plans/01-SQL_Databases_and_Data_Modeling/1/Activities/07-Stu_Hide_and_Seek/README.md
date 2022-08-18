# Hide and Seek 

In this activity, you will create a new database and a new table, then import data from a CSV file. To learn more about this dataset, you may review the reference at the end of this document.

## Instructions

1. In pgAdmin, create a new database called `Miscellaneous_DB`.

2. Open the `soft-attributes.csv` CSV file from the Resources folder to analyze the data. 

3. Using the column headers and data types from the CSV file, write the table schema to create a new table in the `Miscellaneous_DB` database called `movie_words_comparison`.

4. Import the data from the `soft-attributes.csv` file in the Resources folder.

5. Create a query that collects all rows where `Home Alone (1990)` is in the `reference_title` column. 

6. Create a query that collects all rows where the "rater_id" is between 10 and 15.

7. Create a query that searches for the words `artsy` and `heartfelt` in the `soft_attribute` column.

## Bonus

1. Create a query that will collect all rows with a reference title of `Batman (1989)` and a soft attribute of `scary`.

2.  Create a query that will collect all rows with a rater within the 30-40 range and has a reference title of `Home Alone (1990)` and a soft attribute of `artsy`.

## References

Krisztian Balog, Filip Radlinski and Alexandros Karatzoglou from Google LLC (2021). SoftAttributes: Relative movie attribute dataset for soft attributes. [https://github.com/google-research-datasets/soft-attributes](https://github.com/google-research-datasets/soft-attributes).

- - -

Copyright 2022 2U. All Rights Reserved.
