from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from examples.utils import *

input_data = "./data.json"
output_data = "./output"

def dfFromJson():

    jsonString = """{"fields": [{
                                    "field": "body",
                                    "value": ["Some text",
                                    "Another line of text",
                                    "Third line of text."]
                                },
                                {
                                    "field": "urlhash",
                                    "value": "0a0a341e189cf2c002cb83b2dc529fbc454f97cc"
                                }],
                      "score": 0.87475455,
                      "siteId": "9222270286501375973",
                      "id": "0a0a341e189cf2c002cb83b2dc529fbc454f97cc"
                    }                 
                """

    write_to_file(input_data, jsonString)
    df = spark.read.option("multiLine", True).json(input_data)
    # Before applying exploded function
    df.printSchema()
    df.show(20, False)
    exploded_df = df.select(
         col("score")
        ,col("siteId")
        ,col("id")
        ,explode(col("fields")).alias("fields_exploded")
        ,to_json(col("fields_exploded")).alias("fields_json")
    )

    # After applying exploded function
    exploded_df.printSchema()
    exploded_df.show(10,False)
    write_df(exploded_df,output_data)

    # clean up
    delete_file(input_data)
    drop_folder(output_data)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_app").getOrCreate()
    dfFromJson()
