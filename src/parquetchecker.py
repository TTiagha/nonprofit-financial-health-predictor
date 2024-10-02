import pandas as pd
import pyarrow.parquet as pq

# Path to your Parquet file
file_path = r"C:\Users\tiagh\PythonProjects\nonprofit-financial-health-predictor\src\sample.parquet"

# Read the Parquet file
table = pq.read_table(file_path)

# Convert to pandas DataFramemlk
df = table.to_pandas()

# Display basic information about the DataFrame
print("DataFrame Info:")
print(df.info())
print("\n")

# Show all columns and their values
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Don't wrap
pd.set_option('display.max_colwidth', None)  # Show full content of each cell

print("All data:")
print(df)
print("\n")

# Display data types
print("Data Types:")
print(df.dtypes)
print("\n")

# Print specific fields
print("Specific Fields:")
print("EIN:", df['EIN'].values[0])
print("TaxYr:", df['TaxYr'].values[0])
print("Organization Name:", df['OrganizationName'].values[0])
print("State:", df['State'].values[0])
print("Total Revenue:", df['TotalRevenue'].values[0])
print("Total Assets EOY:", df['TotalAssetsEOY'].values[0])




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
