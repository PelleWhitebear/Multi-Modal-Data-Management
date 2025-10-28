import logging
from dotenv import load_dotenv, find_dotenv
from global_scripts.utils import *

load_dotenv(find_dotenv())

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library

    s3_client = minio_init()
    logging.info("Connected to MinIO.")

    try:
        # List all buckets
        buckets = s3_client.list_buckets()["Buckets"]

        if not buckets:
            logging.info("No buckets found. MinIO is already empty.")
            return
        
        for bucket in buckets:
            delete_items(s3_client, bucket["Name"])
            s3_client.delete_bucket(Bucket=bucket["Name"])
            logging.info(f"Bucket '{bucket['Name']}' deleted.")

        logging.info("All data removed. MinIO is empty.")

    except Exception:
        logging.exception(f"Error during deletion process.")
        return



if __name__ == "__main__":
    main()