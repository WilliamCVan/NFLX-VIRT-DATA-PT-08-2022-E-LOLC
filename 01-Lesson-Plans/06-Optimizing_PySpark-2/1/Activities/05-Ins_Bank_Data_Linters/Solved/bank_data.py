
# Import findspark() and initialize. 
import findspark
findspark.init()

# Import other dependencies. 
from pyspark import SparkFiles
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("sparkBankData").getOrCreate()

# Create the import_data function. 
def import_data():
  url = "https://2u-data-curriculum-team.s3.amazonaws.com/nflx-data-science-adv/week-6/zip_bank_data.csv"
  spark.sparkContext.addFile(url)
  df = spark.read.csv(SparkFiles.get("zip_bank_data.csv"), sep=",", header=True)
  df.createOrReplaceTempView('zip_bank_data')

  return df

def transform_data():
    transformed_df = spark.sql("""
    SELECT
        ZIPCODE,
        ADDRESS
    FROM ZIP_BANK_DATA
    LIMIT 10
    """)

    return transformed_df

def transform_data_full():
    transformed_df = spark.sql("""
    SELECT
        ZIPCODE,
        ADDRESS
    FROM ZIP_BANK_DATA
    """)

    return transformed_df

def distinct_zip_codes():
    distinct_zips = spark.sql("""
    SELECT DISTINCT
        ZIPCODE
    FROM ZIP_BANK_DATA
    """)

    return distinct_zips