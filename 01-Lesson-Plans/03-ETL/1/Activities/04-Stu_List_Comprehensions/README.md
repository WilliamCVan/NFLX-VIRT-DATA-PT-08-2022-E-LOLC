# List Comprehensions

In this activity, you’ll practice using list comprehensions.

## Instructions

1. Use a list comprehension to create a list of numbers up to 100 that are divisible by 3.

    * **Hint:** Remember that the modulo operator (`%`), as in `x % y`, gives the remainder if x is divided by y. For example, `x % 2` gives 0 if x is even and 1 if x is odd.

2. Use a list comprehension to create a list of numbers up to 100 that are divisible by either 3 or 5.

3. Use conditional list comprehensions to create a list of all the pet dog names from the following code:

    ```python
    pets = [{
    'name': 'Fido',
    'age': 4,
    'type': 'dog'
    },
    {
    'name': 'Rex',
    'age': 2,
    'type': 'dog'
    },
    {
    'name': 'Puddles',
    'age': 7,
    'type': 'wombat'
    },
    {
    'name': 'Tuna',
    'age': 6,
    'type': 'cat'
    },
    {
    'name': 'Springer',
    'age': 5,
    'type': 'dog'
    }]
    ```

4. Use a list comprehension to create a list that contains three items: a "customer_id", "name", and "email" for each customer in the following `customers` list:

    ```python
    customers = ["400021,Adrianna Raysor,adrianna.raysor@360.cn",
    "400064,Amberly Kerfie,a.kerfie@test.com",
    "400042,Angelique Kleiner,a.klein@example.com",
    "400059,Arleen Bergman,arleen.bergman@friendme.com",
    "400006,Arlinda McBrayer,amcbrayer@yoyo.com",
    "400014,Astrid Brewster,a.brewster@east.com",
    "400027,Ayanna Filippelli,ayanna@readit.com"]
    ```

5. Use a list comprehension to extract the "customer_id" from each customer in the `customers` list, and then convert the "customer_id" to an integer.

6. Use a list comprehension to split each customer "name" into a first and a last name.

- - -

Copyright 2022 2U. All Rights Reserved.
