import requests
import os

# Constants for the data source and destination
URL = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/chess-transfers/transfers.csv"
FILE_PATH = "data/transfer.csv"

def extract(url=URL, file_path=FILE_PATH):
    """Download a file from a URL to the specified file path."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure directory exists
    response = requests.get(url)
    response.raise_for_status()  # Raise exception for HTTP errors

    # Write the file to disk
    with open(file_path, "wb") as file:
        file.write(response.content)
    print(f"Data extracted to {file_path}")
    return file_path

if __name__ == "__main__":
    extract()
