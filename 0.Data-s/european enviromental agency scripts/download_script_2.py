import pandas as pd
import pyarrow.parquet as pq
import requests
import os

import pandas as pd
import pyarrow.parquet as pq
import requests
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time

# Function to get list of parquet files
def get_parquet_files():
    parquet_dir = "0.Data/air pollution data/temp"
    parquet_files = []
    for filename in os.listdir(parquet_dir):
        if filename.endswith(".parquet"):
            parquet_files.append(f"0.Data/air pollution data/temp/{filename}")
    return parquet_files

already_downloaded = get_parquet_files()

# Function to download a file from a given URL
def download_file(url):
    try:
        parquet_file_name = os.path.basename(url)
        save_path = f"0.Data/air pollution data/temp/{parquet_file_name}"

        if save_path not in already_downloaded:
            # print("already downloaded:", save_path)
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
            parquet_file_name = os.path.basename(parquet_file)
            csv_file_name = parquet_file_name.replace('.parquet', '.csv')
            csv_file_path = f"0.Data/air pollution data/csvs/{csv_file_name}"

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
    print(len(urls))
    time.sleep(2)
    # # Download files using threads (I/O-bound task)
    # with ThreadPoolExecutor(max_workers=10) as download_executor:
    #     parquet_files = list(download_executor.map(download_file, urls))
    
    # Convert files using processes (CPU-bound task)
    with ThreadPoolExecutor(max_workers=10) as convert_executor:
        convert_executor.map(convert_parquet_to_csv, already_downloaded)

input_csv = '0.Data/air pollution data/urls_csvs/ParquetFilesUrls_sub.csv'  # Path to the CSV file with URLs
process_csv_with_urls(input_csv)
