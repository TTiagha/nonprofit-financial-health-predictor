import pandas as pd
import pyarrow.parquet as pq

# Replace with your downloaded file path
parquet_file_path = r"C:\Users\tiagh\PythonProjects\nonprofit-financial-health-predictor\src\sample.parquet"

# Read the Parquet file
table = pq.read_table(parquet_file_path)
df = table.to_pandas()

# Display the DataFrame
print(df.head())
print(df.info())
