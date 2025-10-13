import boto3
import logging
from utils import *
from consts import *

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - [%(levelname)s] - %(message)s', 
                    filename='landing_zone/logs/create.log', 
                    force=True,
                    filemode='w')   # Overwrite log file on each run

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

        # Create the bucket and the sub-buckets
        create_bucket(s3_client, LANDING_ZONE_BUCKET)
        create_sub_bucket(s3_client, LANDING_ZONE_BUCKET, TEMPORAL_SUB_BUCKET)
        create_sub_bucket(s3_client, LANDING_ZONE_BUCKET, PERSISTENT_SUB_BUCKET)

    except Exception:
        logging.exception("Error connecting to MinIO.")
        return
    


if __name__ == "__main__":
    main()