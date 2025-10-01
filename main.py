import boto3
from botocore.exceptions import ClientError

from utils import *

def main():

    # Landing zone

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="ROOTNAME",
        aws_secret_access_key="CHANGEME123",
    )

    # Create the bucket and the sub-buckets

    landing_zone_bucket = "landing-zone"
    temporal_landing_sub_bucket = "temporal_landing"
    persistent_landing_sub_bucket = "persistent_landing"
    repeated_sub_bucket = "temporal_landing" # Test already existing sub-bucket

    create_bucket(s3_client, landing_zone_bucket)
    create_sub_bucket(s3_client, landing_zone_bucket, temporal_landing_sub_bucket)
    create_sub_bucket(s3_client, landing_zone_bucket, persistent_landing_sub_bucket)
    create_sub_bucket(s3_client, landing_zone_bucket, repeated_sub_bucket)




if __name__ == "__main__":
    main()