import pyarrow.parquet as pq
import pandas as pd
pd.set_option('display.max_rows', 700)
pd.set_option('display.max_columns', 700)
pd.set_option('display.width', 1000)

data = pq.read_pandas('/Users/miguel.ojeda/Documents/Projects/Oportun/POC/Python/Parquet_files/part-00006-d43cc6b2-1aad-45c9-ba38-ba3c2c0b9a0b.c000.snappy.parquet').to_pandas()
print(type(data))
print(data)