# Data Wrangling Tutorial in Python: Pandas vs. Pyspark

## Getting started: Installation of required libraries
In order to ensure pandas is installed
```bash
pip install pandas
```

In order to ensure pyspark is installed, make sure you have java installed:
```bash
java -version
```

If it isn't, make sure to install it. On Ubuntu, you can use 
```bash
apt install openjdk-11-jre-headless
```

Then you can install pyspark:
```bash
pip install pyspark
```

## Introduction
The examples for this sectionn are available in [the introduction file](introduction.py).

### Creating DataFrames
Let's start with creating an empty DataFrame:

In pandas we can do it with:

```python
import pandas as pd

df = pd.DataFrame(
    {
        "a": [4, ,5, 6],
        "b": [7, 8, 9],
        "c": [10, 11, 12]
    },
    index = [1, 2, 3]
)

print(df)
```
which will yield the following:
```bash
   a  b   c
1  4  7  10
2  5  8  11
3  6  9  12
```


In pyspark, the syntax is similar:

```python
from pyspark.sql import SparkSession

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
```
which will yield the following:
```bash
+-----+---+---+---+                                                             
|index|  a|  b|  c|
+-----+---+---+---+
|    1|  4|  7| 10|
|    2|  5|  8| 11|
|    3|  6|  9| 12|
+-----+---+---+---+
```

### Selecting Columns

Often, we may want to limit the columns we are working with on a DataFrame