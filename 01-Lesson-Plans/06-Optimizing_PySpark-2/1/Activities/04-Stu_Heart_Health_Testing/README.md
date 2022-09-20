# Heart Health Function Testing

In this activity, you will be writing PyTests on the transformation of a heart health dataset.

### Instructions

1. Using starter code provided, transform the Heart Health dataset by writing two functions that do the following:

    * Retrieves the distinct states.
    * Retrieves the states with an average death rate greater than 400.

2. Write the following test to ensure your SQL statements are correct and the data doesn't have any issues.

    * The row count of the source dataset equals 799.

    * The column count of the source dataset equals 9. 

    * The `STATE` column is returned out of the `get_states()` function.

    * The row count of the `get_states()` function is equal to 5.

    * The row count of the `get_states_with_above_average_death_rate()` function is equal to 5.

    * The column count of the `get_states_with_above_average_death_rate()` function is equal to 2.

    * The number of rows is not different between `get_states()` and `get_states_with_above_average_death_rate()`.

---

Copyright 2022 2U. All Rights Reserved.