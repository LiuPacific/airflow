import time

import requests
import sys

print("aa")

def download_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        filename = url.split('/')[-1]
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded: {filename}")
    else:
        print(f"Failed to download file from {url}")


def check_internet_connection():
    try:
        response = requests.get("https://www.google.com", timeout=3)
        response.raise_for_status()
        return True
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        return False


if __name__ == "__main__":
    print("arguments passed to the script are :", sys.argv)
    if len(sys.argv) < 2:
        print("Usage: python download_files.py <URL>")
        sys.exit(1)

    url = sys.argv[1]

    if check_internet_connection():
        print("Internet connection is available.")
    else:
        print("Internet connection is not available.")
    download_file(url)

