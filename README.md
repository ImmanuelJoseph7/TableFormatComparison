# SQL Server → Delta Table Migration Demo

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure credentials:
   ```bash
   cp .env.example .env
   # Edit .env with your Kaggle API key and SQL Server connection string
   ```
   - Kaggle API key: [kaggle.com/settings](https://www.kaggle.com/settings) → API → Create New Token
   - SQL Server: any local or remote instance with a database you can write to

3. Run in order:
   ```bash
   python 00_download_data.py     # Download NYC Parking Violations CSVs from Kaggle (~8.5GB, ~42M rows)
   python 01_migrate_to_delta.py  # CSV → Delta Table, print size comparison
   python 02_optimize_vacuum.py   # Compact files (OPTIMIZE) + remove old versions (VACUUM)
   python 03_encoding_demo.py     # Dictionary encoding breakdown
   ```
