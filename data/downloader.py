"""
This script downloads Ethereum transaction logs from the AWS S3 bucket 'aws-public-blockchain' for a specified date range.
It uses the Boto3 library to interact with AWS S3 and downloads the logs to a local directory.
The script iterates through each day in the specified date range, constructs the S3 object key based on the date,
and downloads the corresponding log files to a local directory.
"""

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import date, timedelta
import os
import polars as pl
import time

bucket_name = 'aws-public-blockchain'
prefix = 'v1.0/eth/logs/'
local_directory = 'logs'
start_date = date(2023, 1, 25)
end_date = date(2026, 4, 30)
delta = timedelta(days=1)

AAVE_POOL_ADDRESS = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"

session = boto3.Session()
s3 = session.client('s3', config=Config(signature_version=UNSIGNED))

# Create download directory if it doesn't exist
if not os.path.exists(local_directory):
    os.makedirs(local_directory)

current_date = start_date
while current_date <= end_date:
    try:
        print(f"Processing date: {current_date}...")
        current_date_str = current_date.strftime('%Y-%m-%d')
        current_prefix = prefix + 'date=' + current_date_str + '/'
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=current_prefix)
        if 'Contents' in response:
            # Create a directory for the current date if it doesn't exist
            current_download_path = os.path.join(local_directory, current_date_str)
            if not os.path.exists(current_download_path):
                os.makedirs(current_download_path)
            counter = 0
            for obj in response['Contents']:
                object_key = obj['Key']
                current_file_path = os.path.join(current_download_path, os.path.basename(object_key))
                print(f"Downloading {object_key} to {current_file_path}...")
                s3.download_file(bucket_name, object_key, current_file_path)
                #
                output_filename = os.path.join(current_download_path, f"pool_data_{current_date_str}_{counter}.parquet")
                df = pl.read_parquet(current_file_path)
                filtered_df = (
                    df.filter(pl.col('address') == AAVE_POOL_ADDRESS)
                    .select("block_timestamp", "block_number", "transaction_index", "log_index", "address", "topics", "data")
                    .sort(["block_number", "transaction_index", "log_index"])
                )
                filtered_df.write_parquet(output_filename)
                counter += 1
                os.remove(current_file_path) # Remove the original parquet file after processing
        else:
            print(f"No objects found under the specified prefix for date {current_date}.")
    except Exception as e:
        print(f"Error occurred while listing objects for date {current_date}: {e}")
    current_date += delta
    time.sleep(1)