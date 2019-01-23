from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def parseJson():
    jsonDF = spark.range(1) \
        .selectExpr("""
            '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString
            """)

    jsonDF.select(
        get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("2nd_elementIn_myJSONValue"),
        json_tuple(col("jsonString"), "myJSONKey").alias("FullColValue")
    ).show(2,False)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("tmp").getOrCreate()
    parseJson()
