import boto3

# TODO: to be removed, should not be submitted in assignment! nice to have for testing

# Connect to MinIO
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="ROOTNAME",
    aws_secret_access_key="CHANGEME123",
)

# List all buckets
buckets = s3.list_buckets()["Buckets"]

for b in buckets:
    name = b["Name"]
    print(f"Deleting bucket: {name}")

    # Delete all objects inside
    objects = s3.list_objects_v2(Bucket=name)
    if "Contents" in objects:
        for obj in objects["Contents"]:
            s3.delete_object(Bucket=name, Key=obj["Key"])
            print(f"  Deleted object: {obj['Key']}")

    # Delete the bucket itself
    s3.delete_bucket(Bucket=name)
    print(f"  Bucket '{name}' deleted.")

print("All data removed. MinIO is empty.")