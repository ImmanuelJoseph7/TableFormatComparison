"""
00_download_data.py
Authenticate with the Kaggle API, download NYC Parking Violations, and
combine all fiscal year CSVs into a single source.csv.
"""

import os
import glob
from dotenv import load_dotenv

load_dotenv()

import kaggle
kaggle.api.authenticate()

os.makedirs("data", exist_ok=True)

print("Downloading NYC Parking Violations from Kaggle...")
kaggle.api.dataset_download_files(
    "new-york-city/nyc-parking-tickets",
    path="data/",
    unzip=True,
)

# Combine all fiscal year CSVs into one source.csv
csv_files = sorted(glob.glob("data/Parking_Violations_Issued_*.csv"))
print(f"\nCombining {len(csv_files)} files into nyc_parking_violations.csv...")

out_path = "data/nyc_parking_violations.csv"
with open(out_path, "wb") as out:
    for i, f in enumerate(csv_files):
        with open(f, "rb") as src:
            # Only write header from the first file
            if i > 0:
                src.readline()
            out.write(src.read())
        print(f"  Merged: {os.path.basename(f)}")

print(f"\nDone. Combined file saved to {out_path}")
print(f"Total size: {os.path.getsize(out_path) / 1024**3:.1f} GB")
