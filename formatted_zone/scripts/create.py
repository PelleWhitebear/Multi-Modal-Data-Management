import sys
import os
import boto3
import logging

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../global_scripts'))
sys.path.append(parent_dir)
from utils import *
from consts import *

setup_logging("create_formatted.log")

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

        # Create the bucket and the main sub-buckets
        create_bucket(s3_client, FORMATTED_ZONE_BUCKET)
        create_sub_bucket(s3_client, FORMATTED_ZONE_BUCKET, JSON_SUB_BUCKET)
        create_sub_bucket(s3_client, FORMATTED_ZONE_BUCKET, MEDIA_SUB_BUCKET)

    except Exception:
        logging.exception("Error connecting to MinIO.")
        return
    


if __name__ == "__main__":
    main()