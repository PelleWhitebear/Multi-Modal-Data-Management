import os
import logging
from datetime import datetime
from botocore.exceptions import ClientError

def create_bucket(s3_client, bucket):
    """Create an S3 bucket. Slighty modified version of this, now also handling already exisiting buckets:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html#create-an-amazon-s3-bucket

    :param s3_client: The S3 client connection
    :param bucket_name: Bucket to create
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"Bucket '{bucket}' already exists.")
        else:
            logging.error(e)
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
        print(f"Sub-bucket '{key}' already exists.")
        return False
    
    except ClientError as e:
        # If error is 404, then bucket does not exist.
        if e.response["Error"]["Code"] == "404":
            s3_client.put_object(Bucket=bucket, Key=f"{key}/")
            print(f"Sub-bucket '{key}' created.")
            return True
        
        else:
            logging.error(e)
            return False
        
def ingest_data(s3_client, bucket, sub_bucket, data_folder):
    """
    Upload new files from data folder to the temporal sub bucket.

    :param s3_client: The S3 client connection
    :param bucket: The parent bucket
    :param sub_bucket: The sub bucket
    :param data_folder: The data folder that the raw data is located in
    :return: True, else False
    """
    for filename in os.listdir(data_folder):
        path = os.path.join(data_folder, filename)
        key = f"{sub_bucket}/{filename}"

        # Check if key exist in bucket
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            print(f"Skipping already uploaded file: {filename}")
            continue
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                print(f"Error checking {filename}: {e}")
                continue

        # Upload the file
        try:
            s3_client.upload_file(path, bucket, key)
            print(f"Uploaded {filename} to {bucket}/{key}")
        except Exception as e:
            logging.error(e)

def move_to_persistent(s3_client, bucket, temporal_sub_bucket, persistent_sub_bucket, data_source):
    """
    Move files from temporal landing to persistent landing, applying naming convention.
    At the end, we delete the original raw data from temporal.
    """
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{temporal_sub_bucket}/")

    if "Contents" not in objects:
        print("No files in temporal landing zone.")
        return

    for obj in objects["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):
            continue

        # New name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = key.split("/")[-1]
        new_key = f"{persistent_sub_bucket}/{data_source}/{data_source}_{timestamp}_{filename}"

        # Copy and move to persistent
        try:
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=new_key
            )
            print(f"Moved {filename} from {key} to {new_key}")
        except Exception as e:
            logging.error(e)
            continue

        # Delete from temporal
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"Deleted {key} from temporal landing.")
        except Exception as e:
            logging.error(e)