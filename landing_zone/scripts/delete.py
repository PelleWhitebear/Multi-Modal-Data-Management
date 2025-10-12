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
            # List all buckets
            buckets = s3_client.list_buckets()["Buckets"]

            if not buckets:
                logging.info("No buckets found. MinIO is already empty.")
                return
            
            for b in buckets:
                name = b["Name"]
                logging.info(f"Deleting bucket: {name}")

                # Delete all objects inside
                objects = s3_client.list_objects_v2(Bucket=name)
                if "Contents" in objects:
                    for obj in objects["Contents"]:
                        s3_client.delete_object(Bucket=name, Key=obj["Key"])
                        logging.info(f"  Deleted object: {obj['Key']}")

                # Delete the bucket itself
                s3_client.delete_bucket(Bucket=name)
                logging.info(f"  Bucket '{name}' deleted.")

            logging.info("All data removed. MinIO is empty.")

        except Exception as e:
            logging.error(f"Error during deletion process: {e}")
            return

    except Exception as e:
        logging.error(f"Error connecting to MinIO: {e}")
        return
    


if __name__ == "__main__":
    main()