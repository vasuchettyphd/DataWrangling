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
The examples for this section are available in [the Pandas introduction file](introduction_pandas.py) and [the Pyspark introduction file](introduction_pyspark.py).

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

### Dropping or Selecting Columns

Often, we may want to limit the columns we are working with on a DataFrame, this can be accomplished by either dropping columns you don't need or selecting columns you need.

In pandas, selecting columns is done by:
```python
subset_df = df[["a", "b"]]
print(subset_df)
```
which yields:
```bash
   a  b
1  4  7
2  5  8
3  6  9
```
Note that you are whether you are selecting multiple columns or a single column, we pass a list into the dataframe selector with the names of the columns you want to select.

Dropping columns is achieved by:
```python
subset_df = df.drop(columns=["a"])
print(subset_df)
```
which yields:
```python
   b   c
1  7  10
2  8  11
3  9  12
```

In Pyspark, the syntax for selection is:
```python
subset_sparkdf = spark_df.select("a", "b")
subset_sparkdf.show()
```
which yields:
```bash
+---+---+
|  a|  b|
+---+---+
|  4|  7|
|  5|  8|
|  6|  9|
+---+---+
```
Note that the selection does not take a list, each column is a separate parameter.

Dropping is accomplished similarly:
```python
subset_sparkdf = spark_df.drop("a", "b")
subset_sparkdf.show()
```