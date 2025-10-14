import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError
from urllib3.exceptions import HTTPError
from consts import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

def create_bucket(s3_client, bucket):
    """Create an S3 bucket. Slighty modified version of this, now also handling already exisiting buckets:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html#create-an-amazon-s3-bucket

    :param s3_client: The S3 client connection
    :param bucket_name: Bucket to create
    :return: True if bucket created, else False
    """
    try:
        s3_client.create_bucket(Bucket=bucket)
        logging.info(f"Bucket '{bucket}' created.")
    except (ClientError, BotoCoreError) as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logging.info(f"Bucket '{bucket}' already exists.")
        else:
            logging.error(f"Unexpected error creating bucket '{bucket}': {e}")
            return False
    except Exception:
        logging.exception(f"Unexpected error creating bucket '{bucket}'.")
        return False
    return True

def create_sub_bucket(s3_client, bucket, key):
    """Create a sub-bucket inside S3 bucket. Slighty modified version of this, now also handling already exisiting buckets:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html#create-an-amazon-s3-bucket

    :param s3_client: The S3 client connection
    :param bucket: The parent bucket
    :param bucket_name: Bucket to create
    :return: True if bucket created, else False
    """
    try:
        # Check if sub-bucket exists, if not error, it does. Sub-buckets are not real buckets, but prefixes inside a bucket.
        s3_client.head_object(Bucket=bucket, Key=f"{key}/")
        logging.info(f"Sub-bucket '{key}' already exists.")
        return False
    
    except (ClientError, BotoCoreError) as e:
        # If error is 404, then bucket does not exist.
        if e.response["Error"]["Code"] == "404":
            s3_client.put_object(Bucket=bucket, Key=f"{key}/")
            logging.info(f"Sub-bucket '{key}' created.")
        else:
            logging.error(f"Unexpected error creating sub-bucket '{key}': {e}")
            return False
    except Exception:
        logging.exception(f"Unexpected error creating sub-bucket '{key}'.")
        return False
    return True

def delete_bucket_elements(s3_client, bucket):
    """
    Delete all objects inside a bucket.

    :param s3_client: The S3 client connection
    :param bucket: The bucket to delete contents from
    :return: True if deletion succeeded, else False
    """
    try:
        logging.info(f"Deleting contents of bucket: {bucket['Name']}.")
        objects = s3_client.list_objects_v2(Bucket=bucket['Name'])
        if "Contents" in objects:
            s3_client.delete_objects(
                Bucket=bucket['Name'],
                Delete={
                    'Objects': [{'Key': obj['Key']} for obj in objects['Contents']],
                    'Quiet': True
                })
            logging.info(f"  Deleted objects.")
        else:
            logging.info(f"  No objects found in bucket: {bucket['Name']}.")
    except Exception:
        logging.exception(f"Unexpected error deleting contents of bucket '{bucket['Name']}'.")
        return False
    return True

def ingest_data(s3_client, bucket, fileobj, key):
    """
    Upload new files from data folder to the temporal sub bucket.

    :param s3_client: The S3 client connection
    :param bucket: The parent bucket
    :param fileobj: The file object to upload
    :param key: The key (including path) inside the bucket where to upload the file object
    :return: True, else False
    """
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket)

    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404" or code == "NoSuchBucket":
            logging.error(f"Bucket '{bucket}' does not exist.")
        else:
            logging.error(f"Error checking if bucket '{bucket}' exists: {e}")
        return False
    except BotoCoreError as e:
        logging.error(f"Error checking if bucket '{bucket}' exists: {e}")
        return False
    except Exception:
        logging.exception(f"Unexpected error checking if bucket '{bucket}' exists.")
        return False

    # Check if object exists in bucket
    try:
        s3_client.head_object(Bucket=bucket, Key=key)

        if key.endswith(".bak"):
            # We always upload backup files, first delete it and then upload it again.
            s3_client.delete_object(Bucket=bucket, Key=key)
        else:
            logging.info(f"Skipping already uploaded file: {key}")
            return False
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            # Error code == 404 means object does not exist, so we can upload it. All other errors are unexpected.
            logging.error(f"Error deleting existing file {key}: {e}")
            return False
    except BotoCoreError as e:
        logging.error(f"Error checking if object '{key}' exists in bucket '{bucket}': {e}")
        return False
    except Exception:
        logging.exception(f"Unexpected error checking if object '{key}' exists in bucket '{bucket}'.")
        return False
        
    # Try uploading the file object
    
    for attempt in range(1, DEFAULT_RETRIES + 1):
        try:
            fileobj.seek(0)
            s3_client.upload_fileobj(fileobj, bucket, key)
            logging.info(f"Uploaded {key} to {bucket}.")
            return True
            
        except HTTPError as e:
            logging.error(f"HTTP error uploading {key} to {bucket}: {e}")
            if attempt == DEFAULT_RETRIES:
                logging.error(f"All {DEFAULT_RETRIES} attempts failed for {key}. Skipping.")
                return False
        except (ClientError, BotoCoreError) as e:
            code = e.response["Error"]["Code"]
            logging.error(f"S3 upload failed ({code}) for {key}: {e}")
            return False
        except Exception:
            logging.exception(f"Unexpected error uploading {key} to {bucket}.")
            return False

def move_to_persistent(s3_client, bucket, temporal_sub_bucket, persistent_sub_bucket, data_source):
    """
    Move files from temporal landing to persistent landing, applying naming convention.
    At the end, we delete the original raw data from temporal.
    """
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{temporal_sub_bucket}/")

    if "Contents" not in objects:
        logging.info("No files in temporal landing zone.")
        return

    for obj in objects["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):
            continue

        # New name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = key.split("/")[-1]
        new_key = f"{persistent_sub_bucket}/{data_source}/{data_source}#{timestamp}#{filename}"

        # Copy and move to persistent
        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=new_key
            )
            logging.info(f"Moved {filename} from {key} to {new_key}")
        except Exception as e:
            logging.error(e)
            continue

        # Delete from temporal
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            logging.info(f"Deleted {key} from temporal landing.")
        except Exception as e:
            logging.error(e)

def setup_logging(log_filename: str, level=logging.INFO, filemode='w'):
    """
    Sets up logging with a file in landing_zone/logs/.
    Ensures the logs directory exists.

    Parameters:
        :param log_filename (str): Name of the log file, e.g., 'delete.log'.
        :param level (int, optional): Logging level, default is logging.INFO.
        :param filemode (str, optional): 'w' to overwrite or 'a' to append, default is 'w'.
    """
    # Ensure logs directory exists
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Full path to log file
    log_file = os.path.join(log_dir, log_filename)

    # Configure logging
    logging.basicConfig(
        level=level,
        format='%(asctime)s - [%(levelname)s] - %(message)s',
        filename=log_file,
        filemode=filemode,
        force=True  # override any existing config
    )