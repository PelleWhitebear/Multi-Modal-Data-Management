import os
import boto3
import logging
from utils import *
from consts import *

log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)  # Create logs directory if it doesn't exist

log_file = os.path.join(log_dir, 'delete.log')

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - [%(levelname)s] - %(message)s', 
                    filename='landing_zone/logs/delete.log', 
                    force=True,
                    filemode='w')  # Overwrite log file on each run

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        logging.info("Connected to MinIO.")

        try:
            # List all buckets
            buckets = s3_client.list_buckets()["Buckets"]

            if not buckets:
                logging.info("No buckets found. MinIO is already empty.")
                return
            
            for bucket in buckets:
                delete_bucket_elements(s3_client, bucket)
                s3_client.delete_bucket(Bucket=bucket["Name"])
                logging.info(f"Bucket '{bucket['Name']}' deleted.")

            logging.info("All data removed. MinIO is empty.")

        except Exception:
            logging.exception(f"Error during deletion process.")
            return

    except Exception:
        logging.exception(f"Error connecting to MinIO.")
        return
    


if __name__ == "__main__":
    main()