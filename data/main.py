import boto3
import gzip
import json
import os
import concurrent.futures
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

BUCKET_NAME = "sbbt"
PREFIX = "bazaar/"
OUTPUT_DIR = "items"

print("setting up s3 client...")
load_dotenv()
r2 = boto3.client(
    service_name="s3",
    endpoint_url=f"https://{os.getenv('CLOUDFLARE_ACCOUNT_ID')}.r2.cloudflarestorage.com",
    aws_access_key_id=os.getenv('R2_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('R2_SECRET'),
    region_name="auto"
)

def download_and_parse_buffer(file_key):
    try:
        timestamp_str = file_key.split('/')[-1].split('.')[0]
        timestamp = int(timestamp_str) // 1000 

        obj = r2.get_object(Bucket=BUCKET_NAME, Key=file_key)

        snapshot_rows = []
        with gzip.GzipFile(fileobj=obj["Body"]) as gzip_file:
            for line in gzip_file:
                item = json.loads(line.decode("utf-8"))
                item["timestamp"] = timestamp
                snapshot_rows.append(item)

        return snapshot_rows
    except Exception as e:
        print(f"Error downloading {file_key}: {e}")
        return None

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    paginator = r2.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=PREFIX
    )

    file_keys = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                file_keys.append(obj["Key"])

    file_keys.sort()

    BATCH_SIZE = 288 
    for i in range(0, len(file_keys), BATCH_SIZE):
        batch_keys = file_keys[i:i+BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1}:")
        batch_rows = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(download_and_parse_buffer, batch_keys)
            for snapshot_rows in results:
                if snapshot_rows:
                    batch_rows.extend(snapshot_rows)

        if not batch_rows:
            continue

        df = pd.DataFrame(batch_rows)
        grouped = df.groupby("productId")

        for prod_id, group in grouped:
            clean_group = group.drop(columns=["productId"])
            table = pa.Table.from_pandas(clean_group, preserve_index=False)

            file_path = os.path.join(OUTPUT_DIR, f"{prod_id}.parquet")

            if os.path.exists(file_path):
                with pq.ParquetWriter(file_path, table.schema, version='2.6', compression='snappy') as writer:
                    writer.write_table(table)
            else:
                with pq.ParquetWriter(file_path, table.schema, version='2.6', compression='snappy') as writer:
                    writer.write_table(table)

        del batch_rows
        del df

    print("Complete!")


if __name__ == "__main__":
    main()
