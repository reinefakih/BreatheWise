import pandas as pd
import os

directory = '0.Data\\air pollution data\\parquets'
parquet_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]

# Create a CSV file and write the header only once
count = 0
for i, parquet_file in enumerate(parquet_files):
    df = pd.read_parquet(parquet_file)
    count += 1
    if i == 0:
        df.to_parquet('0.Data\\combined_file.parquet', index=False, mode='w', header=True)
    else:
        df.to_parquet('0.Data\\combined_file.parquet', index=False, mode='a', header=False)
    print(count)