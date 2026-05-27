from datetime import date, timedelta
import os
import polars as pl

bucket_name = "aws-public-blockchain"
prefix = "v1.0/eth/logs/"
local_directory = "logs"

start_date = date(2023, 1, 25)
end_date = date(2026, 4, 30)
delta = timedelta(days=1)

tmp_file = "pool_data_tmp.parquet"
output_file = "pool_data.parquet"

# Collect lazy parquet scans
lazy_frames = []

current_date = start_date

while current_date <= end_date:
    current_date_str = current_date.strftime("%Y-%m-%d")
    path = os.path.join(local_directory, current_date_str)
    # Skip missing directories
    if not os.path.exists(path):
        print(f"WARNING: Skipping missing directory: {path}")
        current_date += delta
        continue
    for file in os.listdir(path):
        if file.endswith(".parquet"):
            file_path = os.path.join(path, file)
            print(f"Processing file: {file_path}")
            # Lazily scan parquet file
            lazy_frames.append(pl.scan_parquet(file_path))
    current_date += delta

if not lazy_frames:
    raise ValueError("No parquet files found.")

# Concatenate all parquet files
combined = pl.concat(lazy_frames, how="vertical_relaxed")

# Stream directly to a single parquet file
combined.sink_parquet(tmp_file)

# Read back the combined parquet file and sort by block number, transaction index, and log index.
tmp = pl.read_parquet(tmp_file)
tmp = tmp.sort(["block_number", "transaction_index", "log_index"])
tmp.write_parquet(output_file)

print(f"Final parquet file written to: {output_file}")
os.remove(tmp_file)