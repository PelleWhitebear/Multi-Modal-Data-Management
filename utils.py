import logging
# import boto3
from botocore.exceptions import ClientError

# Slighty modified version of this, now also handling already exisiting buckets:
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html#create-an-amazon-s3-bucket
def create_bucket(s3_client, bucket_name):
    """Create an S3 bucket.Slighty modified version of this, now also handling already exisiting buckets:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html#create-an-amazon-s3-bucket

    :param s3_client: The S3 client connection
    :param bucket_name: Bucket to create
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            logging.error(e)
            return False
    return True

## TODO: not sure if this is exactly a 'sub-bucket'
def create_sub_bucket(s3_client, bucket, bucket_name):
    s3_client.put_object(Bucket=bucket, Key="{bucket_name}/")
    print(f"Created sub-bucket: {bucket_name}")