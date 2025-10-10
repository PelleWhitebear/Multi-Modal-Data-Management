import boto3

from utils import *
from consts import *

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="ROOTNAME",
        aws_secret_access_key="CHANGEME123",
    )

    # Landing zone 

    # Create the bucket and the sub-buckets
    create_bucket(s3_client, LANDING_ZONE_BUCKET)
    create_sub_bucket(s3_client, LANDING_ZONE_BUCKET, TEMPORAL_SUB_BUCKET)
    create_sub_bucket(s3_client, LANDING_ZONE_BUCKET, PERSISTENT_SUB_BUCKET)

    # Preparation for incremental data ingestion
    ingest_data(s3_client, LANDING_ZONE_BUCKET, TEMPORAL_SUB_BUCKET, DATA_FOLDER)
    move_to_persistent(s3_client, LANDING_ZONE_BUCKET, TEMPORAL_SUB_BUCKET, PERSISTENT_SUB_BUCKET, DATA_SOURCE)


if __name__ == "__main__":
    main()