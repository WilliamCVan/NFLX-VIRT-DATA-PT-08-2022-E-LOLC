# A View with a Roomful of Queries

In this activity, you’ll work with a partner to practice your join and subquery skills and to build a view.

**Note:** This activity will use the previously created `rental_db` database.

## Instructions

1. Write a query to create a view, named `title_count`, that displays the number of copies of a film title that exists in the inventory. 

2. Using a subquery instead of a join, query the newly created view to find all the titles for which seven copies exist. The result should appear as follows: 

    | | title<br>character varying (255) | Number of Copies<br>bigint |
    |----|----|----|
    | 1 | AFFAIR PREJUDICE | 7 |
    | 2 | ALADDIN CALENDAR | 7 |
    | 3 | ALAMO VIDEOTAPE | 7 |
    | 4 | ALASKA PHANTOM | 7 |
    | 5 | AMISTAD MIDSUMMER | 7 |
    | 6 | ARACHNOPHOBIA ROLLER… | 7 |
    | 7 | ARIZONA BANG | 7 |
    | 8 | ARMAGEDDON LOST | 7 |

2. Write a query to create a view, named `title_count_by_store`, that displays the number of copies of each film title that exists in the inventory for each store. Group the results by `title` and `store_id`, and order them by `title` and "Number of Copies". The view should appear as follows:

    | | title<br>character varying (255) | store_id<br>smallint | Number of Copies<br>bigint |
    |----|----|----|----|
    | 1 | ACADEMY DINOSAUR | 2 | 4 |
    | 2 | ACADEMY DINOSAUR | 1 | 4 |
    | 3 | ACE GOLDFINGER | 2 | 3 |
    | 4 | ADAPTATION HOLES | 2 | 4 |
    | 5 | AFFAIR PREJUDICE | 2 | 3 |
    | 6 | AFFAIR PREJUDICE | 1 | 4 |
    | 7 | AFRICAN EGG | 2 | 3 |
    | 8 | AGENT TRUMAN | 2 | 3 |
    | 9 | AGENT TRUMAN | 1 | 3 |
    | 10 | AIRPLANE SIERRA | 2 | 2 |

3. Write a query using the newly created view that returns the `title` and `store_id`, and creates a new column, "Status", that contains the text "Order 2 more copies" if there are two copies in the inventory for each store, or "Order 1 more copy" if there are three copies in the inventory for each store. If there are four or more copies then the "Status" should be "Okay".

    * **Note:** There are no stores with 1 or 0 copies in their inventory. 

    * **Hint:** In the `CASE` statement you need to write two `WHEN` statements&mdash;one for each condition.

    * The results should appear as follows: 

      | | title<br>character varying (255) | store_id<br>smallint | Status<br>text|
      |----|----|----|----|
      | 1 | ACADEMY DINOSAUR | 2 | Okay |
      | 2 | ACADEMY DINOSAUR | 1 | Okay |
      | 3 | ACE GOLDFINGER | 2 | Order 1 more copy |
      | 4 | ADAPTATION HOLES | 2 | Okay |
      | 5 | AFFAIR PREJUDICE | 2 | Order 1 more copy |
      | 6 | AFFAIR PREJUDICE | 1 | Okay |
      | 7 | AFRICAN EGG | 2 | Order 1 more copy |
      | 8 | AGENT TRUMAN | 2 | Order 1 more copy |
      | 9 | AGENT TRUMAN | 1 | Order 1 more copy |
      | 10 | AIRPLANE SIERRA | 2 | Order 2 more copies |

---

Copyright 2022 2U. All Rights Reserved.