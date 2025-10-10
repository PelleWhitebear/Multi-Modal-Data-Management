import boto3
import logging
from utils import *
from consts import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="ROOTNAME",
            aws_secret_access_key="CHANGEME123",
        )
        logging.info("Connected to MinIO.")

        try:
            move_to_persistent(s3_client, LANDING_ZONE_BUCKET, TEMPORAL_SUB_BUCKET, PERSISTENT_SUB_BUCKET, DATA_FOLDER)
            
        except Exception as e:
            logging.error(f"Error during moving data to persistent storage: {e}")
            return

    except Exception as e:
        logging.error(f"Error connecting to MinIO: {e}")
        return
    


if __name__ == "__main__":
    main()