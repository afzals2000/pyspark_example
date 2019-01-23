from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *
from examples.utils import *

input_data = "./emp.csv"

def df_schema_style_1(spark):
    schema1 = StructType([
        StructField("name", StringType()),
        StructField("age", StringType()),
        StructField("dob", StringType())
    ])

    # df = spark.read.option("header", "true").option("inferSchema", "true").csv("/tmp/emp.csv")
    df = spark.read.option("header", "true").schema(schema1).csv(input_data)
    df.show()


def df_schema_style_2(spark):
    # schema style2
    schema_string = "name age dob"
    fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split()]
    schema2 = StructType(fields)

    df = spark.read.option("header", "true").schema(schema2).csv(input_data)
    df.show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("tmp").getOrCreate()
    employee = """david, 24, "2010-01-01"
john, 24, "2014-01-01"
"""
    write_to_file(input_data, employee)
    df_schema_style_1(spark)
    df_schema_style_2(spark)

    # clean up
    delete_file(input_data)
