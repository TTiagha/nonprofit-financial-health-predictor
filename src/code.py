import pandas as pd
import pyarrow.parquet as pq
from tabulate import tabulate

# Replace with your downloaded file path
parquet_file_path = r"C:\Users\tiagh\PythonProjects\nonprofit-financial-health-predictor\src\sample.parquet"

# Read the Parquet file
table = pq.read_table(parquet_file_path)
df = table.to_pandas()

# Ensure required columns exist
required_columns = ['name', 'ein', 'mission_statement']
if not all(col in df.columns for col in required_columns):
    raise ValueError(f"The Parquet file must contain the following columns: {required_columns}")

# Create a new DataFrame with the desired columns
nonprofit_info_df = df[['name', 'ein', 'mission_statement']]
# Display the original DataFrame
print("Original DataFrame:")
print(df.head())
print(df.info())

# Display the nonprofit information table
print("\nNonprofit Information:")
print(tabulate(nonprofit_info_df, headers='keys', tablefmt='psql'))