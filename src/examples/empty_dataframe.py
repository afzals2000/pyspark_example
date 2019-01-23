from pyspark.sql import SparkSession
from pyspark.sql.types import *


def empty_df_func(spark):

    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([]))
    empty_df.printSchema()
    empty_df.show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_app").getOrCreate()
    empty_df_func(spark)
