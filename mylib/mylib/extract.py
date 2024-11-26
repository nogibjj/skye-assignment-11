import requests
import os

url = "https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/chess-transfers/transfers.csv"
file_path = "data/transfer.csv"


def extract(url=url, file_path=file_path):
    """ "Extract a url to a file path"""

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path
