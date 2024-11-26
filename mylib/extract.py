import requests
import os

# Constants for the data source and destination
URL = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/chess-transfers/transfers.csv"
LOCAL_TEMP_PATH = "/tmp/transfer.csv"  # Temporary local path
DBFS_PATH = "dbfs:/FileStore/skye-assignment-11/transfer.csv"  # Target DBFS path

def extract(url=URL, local_path=LOCAL_TEMP_PATH, dbfs_path=DBFS_PATH):
    """Download a file from a URL and save it to DBFS."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Data downloaded to local path: {local_path}")

    dbutils.fs.cp(f"file://{local_path}", dbfs_path)
    print(f"Data moved to DBFS path: {dbfs_path}")

    return dbfs_path

if __name__ == "__main__":
    extract()
