# Employee Database Analysis

For this assignment, you’ll perform a data analysis on the `Employee_DB` database that you created earlier.

## Before You Begin

* **Note:** For this homework assignment, you’ll use the `Employee_DB` repository that you created earlier.

## Instructions

1. Rename the `employee_database_analysis_starter_code.sql` file to `employee_database_analysis.sql`, and then in the remaining steps, save your queries in that file. 

2. Create a view, named `retirement_info`, of all the employees in the `employees` table that have already retired or will soon be retiring. Such employees were born between 1952 and 1955 (inclusive) and hired between 1985 and 1988 (inclusive).

    * **Hint:** Filter the table by using the `WHERE` clause on the `birth_date` and `hire_date` columns and by using the `BETWEEN` and `AND` conditional statements.

    * The first ten rows of the view should appear as follows:

      | | emp_no<br> [PK] integer | first_name<br>character varying | last_name<br>character varying |
      |----|----|----|----|
      | 1 | 10001 | Georgi | Facello |
      | 2 | 10004 | Christian | Koblick |
      | 3 | 10009 | Sumant | Peac |
      | 4 | 10018 | Kazuhide | Peha |
      | 5 | 10035 | Alain | Chappelet |
      | 6 | 10053 | Sanjiv | Zschoche |
      | 7 | 10058 | Berhard | McFarlin |
      | 8 | 10066 | Kwee | Schusler |
      | 9 | 10067 | Claudi | Stavenow |
      | 10 | 10070 | Reuven | Garigliano |

3. Using the `retirement_info` view, create another view, named `current_employees`, that holds a list of all the current employees, including their employee numbers, their first and last names, and their `to_date` values. (Note that some employees have more than one `to_date` value in the `dept_emp` table. That’s because they changed jobs within the company.)

    * **Hint:** Join the `retirement_info` view with the `dept_emp` table, and then filter for the employees that have a `to_date` value of `9999-1-01`.

    * The first ten rows of the view should appear as follows:

      | | emp_no<br> [PK] integer | first_name<br>character varying | last_name<br>character varying | to_date<br>date |
      |----|----|----|----| ---- |
      | 1 | 10001 | Georgi | Facello | 9999-01-01 |
      | 2 | 10004 | Christian | Koblick | 9999-01-01 |
      | 3 | 10009 | Sumant | Peac | 9999-01-01 |
      | 4 | 10018 | Kazuhide | Peha | 9999-01-01 |
      | 5 | 10035 | Alain | Chappelet | 9999-01-01 |
      | 6 | 10053 | Sanjiv | Zschoche | 9999-01-01 |
      | 7 | 10058 | Berhard | McFarlin | 9999-01-01 |
      | 8 | 10066 | Kwee | Schusler | 9999-01-01 |
      | 9 | 10067 | Claudi | Stavenow | 9999-01-01 |
      | 10 | 10070 | Reuven | Garigliano | 9999-01-01 |

4. Using the `current_employees` view, list the average salary for each job title of all the current employees, and order the results from the lowest to the highest average salary.

    * The list of average salaries by title should appear as follows:

      | | title<br>character varying | Avg. Salary<br>numeric |
      |----|----|----|
      | 1 | Technique Leader | 48089.13 | 
      | 2 | Assistant Engineer |48360.98 |
      | 3 | Senior Engineer | 48510.42 | 
      | 4 | Engineer | 48521.19 | 
      | 5 | Manager | 49503.20 | 
      | 6 | Staff | 58472.09 | 
      | 7 | Senior Staff | 58481.02 | 

5. Using the `current_employees` view, list the average salary for each department of all the current employees, and order the results from the lowest to the highest average salary.

    * The list of average salaries for each department should appear as follows:

      | | dept_name<br>character varying | Avg. Salary<br>numeric |
      |----|----|----|
      | 1 | Human Resources | 45083.14 | 
      | 2 | Quality Management |46388.788 |
      | 3 | Customer Service | 48108.83 | 
      | 4 | Development | 48636.83 | 
      | 5 | Production | 48745.98 | 
      | 6 | Research | 48813.93 | 
      | 7 | Finance | 59852.40 | 
      | 8 | Marketing | 61309.34 | 
      | 9 | Sales | 69587.96 |

6. Using the `current_employees` view, compare the average salary by title for each department of all the current employees, and order the results from the lowest to the highest average salary by title. 

    * The first five rows of the list of average salaries by title for each department should appear as follows:

      | | dept_name<br>character varying | title<br>character varying | Avg. Salary<br>numeric |
      |----|----|----|---|
      | 1 | Customer Service | Assistant Engineer | 43610.55 |
      | 2 | Quality Management | Assistant Engineer | 46380.71 | 
      | 3 | Research | Assistant Engineer | 48014.10 |
      | 4 | Production | Assistant Engineer | 48375.87 |
      | 5 | Development | Assistant Engineer | 48761.93 |

7. Using the `current_employees` view, write a query that ranks the number of current employees by title within each department. 

    * **Hint:** You need to use `PARTITION BY` and `ORDER BY` in the `RANK` window function.

    * The first seven rows of the results should appear as follows: 

      | | title<br>character varying | dept_name<br>character varying | count<br>bigint | rank<br>bigint |
      |----|----|----|----| ---|
      | 1 | Senior Staff | Customer Service | 1895 | 1 |
      | 2 | Staff | Customer Service | 1781 | 2 |
      | 3 | Senior Engineer | Customer Service | 284 | 3 |
      | 4 | Engineer | Customer Service | 265 | 4 |
      | 5 | Assistant Engineer | Customer Service | 92 | 5 |
      | 6 | Technique Leader | Customer Service | 21 | 6 |
      | 7 | Manager | Customer Service | 1 | 7 |

## Grading Requirements

This assignment will be evaluated against the rubric and assigned a grade according to the following table:

| Grade | Points |
| --- | --- |
| High Pass | 90 or more |
| Pass | 70&ndash;89 |
| Fail | 1&ndash;69 |
| Incomplete | 0 |

---

## Submission

Before you submit your assignment, check your work against the rubric to make sure that you’re meeting the requirements.

To submit your assignment for grading in Canvas, click Start Assignment, click the Website URL tab, add the URL of your `Employee_DB` GitHub repository, and then click Submit. If you’d like to resubmit your work, communicate with your instructional team to request another opportunity to improve your grade.

## Reference

Mockaroo, LLC. (2021). Realistic Data Generator. [https://www.mockaroo.com/](https://www.mockaroo.com/)

---

Copyright 2022 2U. All Rights Reserved.