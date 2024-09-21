import pandas as pd
import os
from glob import glob
import pyarrow.parquet as pq

# Directory containing the parquet files
parquet_dir = "C:\\Users\\allab\\OneDrive\\Desktop\\Final Project\\0.Data\\air pollution data\\parquets"
# Output directory for Parquet files
output_dir = "C:\\Users\\allab\\OneDrive\\Desktop\\Final Project\\0.1.Data Used\\air data"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Get a list of all parquet files
parquet_files = glob(os.path.join(parquet_dir, "*.parquet"))
print(len(parquet_files))

# Iterate over all parquet files
def process_parquet(parquet_file):
    try:
        # Read the parquet file into a pandas DataFrame
        df = pq.read_table(parquet_file).to_pandas()
        
        # Ensure the DataFrame has a column for the date or year
        df['year'] = pd.to_datetime(df['End']).dt.year  # Ensure it's a datetime and extract the year

        # Filter data by year and save to corresponding Parquet files
        for year in df['year'].unique():
            # Filter data for the specific year
            df_year = df[df['year'] == year]
            
            # Construct the output file path
            output_file = os.path.join(output_dir, f"{year}.parquet")
            
            # If the file already exists, append to the existing Parquet file
            if os.path.exists(output_file):
                df_existing = pd.read_parquet(output_file)
                df_combined = pd.concat([df_existing, df_year], ignore_index=True)
                df_combined.to_parquet(output_file, index=False)
            else:
                df_year.to_parquet(output_file, index=False)
        
        print(f"Processed file: {parquet_file}")
    
    except Exception as e:
        print(f"Failed to process {parquet_file}: {e}")

for i, parquet_file in enumerate(parquet_files):
    print(f"file {i}/{len(parquet_files)}")
    process_parquet(parquet_file)