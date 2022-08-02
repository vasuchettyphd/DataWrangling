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
