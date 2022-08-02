from pyspark.sql import SparkSession
import pandas as pd

df = pd.DataFrame(
    {
        "a": [4, 5, 6],
        "b": [7, 8, 9],
        "c": [10, 11, 12]
    },
    index = [1, 2, 3]
)

print(df)

spark = SparkSession.builder.appName('datawrangler').getOrCreate()

spark_df = spark.createDataFrame(
    [
        (1, 4, 7, 10),
        (2, 5, 8, 11),
        (3, 6, 9, 12)
    ],
    ["index", "a", "b", "c"]
)

spark_df.show()