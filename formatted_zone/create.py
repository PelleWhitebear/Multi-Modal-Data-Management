import logging
import dotenv
import os
from global_scripts.utils import minio_init, create_bucket, create_sub_bucket

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    s3_client = minio_init()

    # Create the bucket and the main sub-buckets
    create_bucket(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"))
    create_sub_bucket(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"), os.getenv("JSON_SUB_BUCKET"))
    create_sub_bucket(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"), os.getenv("MEDIA_SUB_BUCKET"))


if __name__ == "__main__":
    main()