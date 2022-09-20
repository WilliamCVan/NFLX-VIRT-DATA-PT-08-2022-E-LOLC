# Midterm Project 

## Before You Begin

* Make sure you have cloned the midterm GitHub repository that was created in class. 

## Instructions

* The midterm project is broken down into two phases. 

#### Phase I

* During Phase I, you'll demonstrate how to use data modeling to design a database, schema, and identify data relationships. Then, you'll create an ETL pipeline to extract and transform the data, and load the data into a relational database. Finally, you'll write advanced SQL queries to answer key questions about the data.

  * **Requirements**
    * Extract and transform the data using Python and Pandas.
    * Create a test suite with four tests that test the functionality of the data extraction or transformation process.
    * Use data modeling to design a database and schema, and identify data relationships.
    * Load the data into a Postgres database.
    * Use advanced SQL queries to perform data analysis.


#### Phase II

* During Phase II, you'll showcase your ability to migrate the ETL pipeline to the cloud and test for quality assurance. 

  * **Requirements**
    * Create an PySpark or Spark SQL pipeline that extracts and transforms the data using Google Colaboratory.
    * Demonstrate an increase in query execution times by storing the data in parquet format, partitions, or cache.
    * Write four assertions to test for data quality assurance during the data extraction or transformation process.

## Submission

Before submitting, check your work against the [Midterm Project Grading Rubric](Midterm_Project_Grading_Rubric.pdf) to ensure you are meeting the project requirements. It’s easy to overlook things when you’re in the zone!

You'll need to upload the following items to your midterm project GitHub repository:

1. A final Jupyter notebook named according to the following format:  `<your_initials>_<name_of_project>ETL.ipynb` (for example,`TB_Predicting_NBA_champions_ETL.ipynb`). The final code should contain the following:
    * The data extraction and transformation process using Python and Pandas
    * A PyTest test suite consisting of four tests that pass.
    * Code that exports the final DataFrames as CSV files. 

2. A database ERD saved as an PNG file.

3. The database schema saved as a Postgres `.sql` file.

4. The advanced SQL queries saved as `.sql` file.

5. A final Colab Jupyter notebook named according to the following format:  `<your_initials>_<name_of_project>Spark_ETL.ipynb`. The final code should contain the following: 
    * A PySpark or Spark SQL pipeline that extracts and transforms data. 
    * Code that demonstrates increased query execution time using parquet format, partitions, or caching. 
    * Four successful assertions that test for quality assurance during the data extraction or transformation process.

    * **Important** Do not clear the output of your Jupyter notebook files.

6. Any datasets used. If the datasets are too large to add to GitHub please use a cloud storage services and provide the URL in your Jupyter notebooks. Here are some [free-trial options](https://www.businessinsider.com/free-cloud-storage).

7. A `README.md` that summarizes your major findings. Your findings should be supported with tables and visualizations. 

To submit your assignment for grading in Canvas, click Start Assignment, click the Website URL tab, click the Website URL tab, add the URL for the solution to the midterm project on your GitHub repository, and then click Submit.  If you would like to resubmit your work, talk to your instructional team to request another opportunity to improve your grade.
