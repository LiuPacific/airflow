from minio import Minio
from minio.error import S3Error
import sys

# Initialize the Minio client
client = Minio(
    endpoint="127.0.0.1:9000",  # not 9090
    access_key="osu",
    secret_key="osugeo123",
    secure=False  # Change to True if you're using HTTPS
)


def download_file_fromS3(object_name, bucket_name, saving_dir="."):
    try:
        file_path = f"{saving_dir}/{object_name}"
        # Download the file from the bucket
        client.fget_object(bucket_name, object_name, file_path)
        print(f"Successfully downloaded {object_name} to {file_path}")
    except S3Error as e:
        print(f"Error occurred: {e}")


if __name__ == '__main__':
    # /home/typingliu/.conda/envs/airflow25_dev/bin/python /home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2sfca/S3_downloading/object_storage_download.py osu-geohealth ok_od_travel_3hour.csv
    # arguments passed to the script are : ['/home/typingliu/workspace/tpy/airflow25/airflow/airflow/hara/hara_dags/cwl_2sfca/S3_downloading/object_storage_download.py', 'osu-geohealth', 'ok_od_travel_3hour.csv']
    print("arguments passed to the script are :", sys.argv)
    if len(sys.argv) < 3:
        print("2 parameters are required")
        sys.exit(1)

    # Define the bucket name and object name (file name in the bucket)
    # bucket_name = "osu-geohealth"
    bucket_name = sys.argv[1]
    # object_name = "ok_od_travel_3hour.csv"  # The name of the file you want to download
    object_name = sys.argv[2]
    # Local path to save the downloaded file
    download_file_fromS3(object_name, bucket_name)

