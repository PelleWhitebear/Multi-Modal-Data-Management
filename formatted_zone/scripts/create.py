import boto3
import logging
import dotenv
import os
from global_scripts.utils import create_bucket, create_sub_bucket

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        logging.info("Connected to MinIO.")

        # Create the bucket and the main sub-buckets
        create_bucket(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"))
        create_sub_bucket(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"), os.getenv("JSON_SUB_BUCKET"))
        create_sub_bucket(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"), os.getenv("MEDIA_SUB_BUCKET"))

    except Exception:
        logging.exception("Error connecting to MinIO.")
        return
    


if __name__ == "__main__":
    main()