# Pandas
import pandas as pd

# Create DataFrame
df = pd.DataFrame(
    {
        "a": [4, 5, 6],
        "b": [7, 8, 9],
        "c": [10, 11, 12]
    },
    index = [1, 2, 3]
)
print(df)

# Select columns
subset_df = df[["a", "b"]]
print(subset_df)

# Drop columns
subset_df = df.drop(columns=["a"])
print(subset_df)

# Rename columns
renamed_df = df.rename(columns={
    "a": "column1",
    "b": "column2"
})
print(renamed_df)