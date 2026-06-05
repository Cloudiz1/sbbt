import os
import pandas as pd

file_name = input("Enter product file name (e.g., DIAMOND): ").strip().upper()
df = pd.read_parquet(f"items/{file_name}.parquet")
os.makedirs("out", exist_ok=True)
df.to_csv(f"out/{file_name}.csv", index=False)
