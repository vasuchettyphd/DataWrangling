# Pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('datawrangler').getOrCreate()

# Create DataFrame
spark_df = spark.createDataFrame(
    [
        (1, 4, 7, 10),
        (2, 5, 8, 11),
        (3, 6, 9, 12)
    ],
    ["index", "a", "b", "c"]
)
spark_df.show()

# Select columns
subset_sparkdf = spark_df.select("a", "b")
subset_sparkdf.show()

# Drop columns
subset_sparkdf = spark_df.drop("a", "b")
subset_sparkdf.show()

# Rename columns
renamed_sparkdf = spark_df.withColumnRenamed("a", "column1")
renamed_sparkdf.show()

column_list = ["index", "column1", "column2", "c"]
renamed_sparkdf = spark_df.toDF(*column_list)
renamed_sparkdf.show()

import pyspark.sql.functions as F
renamed_sparkdf = spark_df.select("index", F.col("a").alias("column1"), F.col("b").alias("column2"), "c")
renamed_sparkdf.show()