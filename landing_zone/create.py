import os
import boto3
import logging
import dotenv
from global_scripts.utils import *

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    s3_client = minio_init()

    # Create the bucket and the sub-buckets
    create_bucket(s3_client, os.getenv("LANDING_ZONE_BUCKET"))
    create_sub_bucket(s3_client, os.getenv("LANDING_ZONE_BUCKET"), os.getenv("TEMPORAL_SUB_BUCKET"))
    create_sub_bucket(s3_client, os.getenv("LANDING_ZONE_BUCKET"), os.getenv("PERSISTENT_SUB_BUCKET"))
    


if __name__ == "__main__":
    main()