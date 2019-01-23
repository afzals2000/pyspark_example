from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *


def employees_from_list(spark):
    Employee_List = [
        ("david", "real", 23, datetime(2010, 1, 1), "female", True),
        ("peter", "smith", 23, datetime(2010, 1, 1), "male", False),
        ("nathan", "tucker", 23, datetime(2010, 1, 1), "others", False),
        ("alien", "xyz", 23, None, "others", False)
    ]

    df_employees = spark.createDataFrame(Employee_List, ["first_name", "last_name", "age", "dob", "gender", "status"])

    df_employees.select(
          full_name().alias("full_name")
        , col("dob")
        , expr("substring(first_name, 2, length(first_name)-2)").alias("substr_expr") # sql expression
        , dob_to_microseconds("dob")  # convert date to epoch in microseconds
    ).show()


def dob_to_microseconds(colName):
    return when(col(colName).isNull(), current_timestamp().cast(LongType())).otherwise(lit(1000000)).alias(colName)


def full_name():
    return concat(salutation(col("gender")), col("first_name"), lit(" "), col("last_name"))


def mr_and_miss(gender):
    if gender == "male":
        return "Mr "
    elif gender == "female":
        return "Miss "
    else:
        return ""


salutation = udf(mr_and_miss, StringType())

if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_app").getOrCreate()
    employees_from_list(spark)
