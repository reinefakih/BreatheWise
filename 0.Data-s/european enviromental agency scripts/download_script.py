import pandas as pd
import pyarrow.parquet as pq
import requests
import os

import pandas as pd
import pyarrow.parquet as pq
import requests
import os
from concurrent.futures import ThreadPoolExecutor

# Function to download a file from a given URL
def download_file(url):
    try:
        parquet_file_name = os.path.basename(url)
        save_path = f"./parquets/{parquet_file_name}"
        
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        with open(save_path, 'wb') as file:
            file.write(response.content)

        print(f"Downloaded: {url}")
        return save_path
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}: {e}")
        return None

# Function to convert Parquet to CSV
def convert_parquet_to_csv(parquet_file):
    if parquet_file:
        try:
            csv_file_name = parquet_file.replace('.parquet', '.csv')
            csv_file_path = f"./csvs/{csv_file_name}"

            # Read the parquet file
            table = pq.read_table(parquet_file)
            # Convert to pandas DataFrame
            df = table.to_pandas()
            # Write to CSV
            df.to_csv(csv_file_path, index=False)
            print(f"Converted {parquet_file} to {csv_file_path}")
        except Exception as e:
            print(f"Failed to convert {parquet_file} to CSV: {e}")

# Main function to process the CSV with URLs
def process_csv_with_urls(input_csv):
    # Read the CSV with URLs
    df = pd.read_csv(input_csv)
    urls = df.iloc[:, 0].tolist()  # Assuming the URL is in the first column
    
    # Using ThreadPoolExecutor to parallelize tasks
    with ThreadPoolExecutor(max_workers=5) as executor:
        # First, download all Parquet files concurrently
        parquet_files = list(executor.map(download_file, urls))
        
        # Then, convert the downloaded Parquet files to CSVs concurrently
        executor.map(convert_parquet_to_csv, parquet_files)

input_csv = 'ParquetFilesUrls.csv'  # Path to the CSV file with URLs
process_csv_with_urls(input_csv)
