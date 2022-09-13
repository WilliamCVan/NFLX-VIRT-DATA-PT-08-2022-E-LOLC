CREATE TABLE departments (
	dept_no VARCHAR(50) NOT NULL,
	dept_name VARCHAR(50) NOT NULL,
	PRIMARY KEY (dept_no)
);

CREATE TABLE dept_emp (
	emp_no VARCHAR(50) NOT NULL,
	dept_no varchar(50) NOT NULL,
	from_date INTEGER NOT NULL,
	to_date INTEGER NOT NULL,
	FOREIGN KEY(dept_no));


select * from dept_emp

Dept_manager
-
emp_no int FK >- Employees.emp_no
dept_no varchar(50) FK >- Departments.dept_no
from_date dateTime
to_date dateTime

Employees 
-
emp_no PK int
birth_date dateTime
first_name string
last_name string
gender string
hire_date dateTime

Titles
-
emp_no int FK >- Employees.emp_no
title varchar(50)
from_date dateTime
to_date dateTime

Salaries
-
emp_no int FK >- Employees.emp_no
salary 
from_date dateTime
to_date dateTime
