import requests
import sys


def download_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        filename = url.split('/')[-1]
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded: {filename}")
    else:
        print(f"Failed to download file from {url}")


if __name__ == "__main__":
    print("arguments passed to the script are :", sys.argv)
    if len(sys.argv) != 2:
        print("downloading url is required")
        sys.exit(1)

    url = sys.argv[1]
    download_file(url)
