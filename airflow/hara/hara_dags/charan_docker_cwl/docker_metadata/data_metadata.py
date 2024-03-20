import sys
import pandas as pd


def dataset_metadata(path):
    df = pd.read_csv(path)
    print('***** Dataset Metadata *******')
    print(f'Shape: {df.shape}')
    print(f'Columns: {df.columns.tolist()}')
    print(f'Missing Values:\n{df.isna().sum()}')
    print('******************************')


if __name__ == "__main__":
    print("arguments passed to the script are :", sys.argv)
    if len(sys.argv) != 2:
        print("Usage: python dataset_metadata.py <path>")
        sys.exit(1)

    path = sys.argv[1]
    dataset_metadata(path)
