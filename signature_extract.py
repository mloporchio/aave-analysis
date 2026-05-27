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
output_file = "signatures.tsv"
signature_count = dict()

current_date = start_date
while current_date <= end_date:
    current_date_str = current_date.strftime('%Y-%m-%d')
    path = os.path.join(local_directory, current_date_str)
    for file in os.listdir(path):
        if file.endswith(".parquet"):
            print(f"Processing file: {file}")
            df = pl.read_parquet(os.path.join(path, file))
            for row in df.iter_rows(named=True):
                signature = row["topics"][0]
                if signature in signature_count:
                    signature_count[signature] += 1
                else:
                    signature_count[signature] = 1
    current_date += delta

# Write signatures to output file in sorted order
sorted_signatures = sorted(signature_count.items(), key=lambda x: x[1], reverse=True)
with open(output_file, "w") as f:
    for signature, count in sorted_signatures:
        f.write(f"{signature}\t{count}\n")
