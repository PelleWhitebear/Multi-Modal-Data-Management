import os
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError
from urllib3.exceptions import HTTPError
from chromadb import HttpClient
from google import genai
import json
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

def minio_init():
    """
    Initialize MinIO S3 client using environment variables.

    :return: Configured S3 client
    """
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        if not s3_client:
            logging.error("Failed to create MinIO S3 client.")
            return
        logging.info("Connected to MinIO S3 successfully.")
        return s3_client
    except Exception:
        logging.exception("Error connecting to MinIO.")
        return

def chroma_init():
    """
    Initialize Chroma client using environment variables.

    :return: Configured Chroma client
    """
    try:
        return HttpClient(
            host="chroma",
            port=8000
        )
    except Exception:
        logging.exception("Error connecting to ChromaDB.")
        return

def gemini_init():
    """
    Initialize Google Gemini client using environment variables.

    :return: Configured Gemini client
    """
    try:
        return genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    except Exception:
        logging.exception("Error connecting to Google Gemini.")
        return

def query_gemini(gemini_client, prompt, config=None):
    """
    Query Google Gemini with the given prompt.

    :param gemini_client: The Gemini client connection
    :param prompt: The prompt string to send to Gemini
    :return: The response from Gemini
    """
    try:
        if config:
            response = gemini_client.models.generate_content(
                model=os.getenv("GEMINI_MODEL"),
                contents=prompt,
                config=config
            )
        else:
            response = gemini_client.models.generate_content(
                model=os.getenv("GEMINI_MODEL"),
                contents=prompt
            )
        return response.text
    except Exception:
        logging.exception("Error querying Google Gemini.")
        return

def query_chromadb(chroma_client, query_type, query, collection, k):
    """
    Query ChromaDB with the given query and collection.

    :param chroma_client: The Chroma client connection
    :param query_type: The type of the query -> [text, image, video]
    :param query: The query string to send to ChromaDB
    :param collection: The collection name to query -> [text, image, video]
    :param k: The number of results to return
    :return: The results from ChromaDB
    """
    try:
        # Get collection
        chroma_collection = None
        collections = chroma_client.list_collections()
        for coll in collections:
            if collection in coll.name:
                chroma_collection = chroma_client.get_collection(coll.name)
                break

        if not chroma_collection:
            logging.error(f"ChromaDB collection '{collection}' does not exist.")
            return []
        
    except Exception:
        logging.exception(f"Error accessing ChromaDB collection '{collection}'.")
        return []
    
    try:
        # Query collection
        if query_type == "text":
            results = chroma_collection.query(
                query_texts=[query],
                n_results=k
            )
        elif query_type == "image":
            results = chroma_collection.query(
                query_images=[query],
                n_results=k
            )
        elif query_type == "video":
            results = chroma_collection.query(
                query_images=[query],
                n_results=k
            )
        return [(id, distance) for id, distance in zip(results["ids"][0], results["distances"][0])]
    except Exception:
        logging.exception(f"Error querying ChromaDB collection '{collection}'.")
        return []

def load_games_from_minio(s3_client, bucket, prefix, suffix):
    """
    Load games JSON file from MinIO S3.

    :param s3_client: The S3 client connection
    :param bucket: The bucket name
    :param prefix: The prefix path inside the bucket
    :param suffix: The suffix (file name) to load
    :return: The loaded JSON data
    """
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in objs:
            logging.error("No JSON files found in exploitation-zone.")
            return {}
        
        for obj in objs["Contents"]:
            if obj["Key"].endswith(suffix):
                game_obj = s3_client.get_object(Bucket=bucket, Key=obj["Key"])
                games = json.loads(game_obj["Body"].read().decode("utf-8"))
                return games
        logging.error(f"No file ending with '{suffix}' found in bucket '{bucket}' with prefix '{prefix}'.")
        return {}

    except Exception:
        logging.exception("Error fetching game JSON from MinIO.")
        return

def create_bucket(s3_client, bucket):
    """Create an S3 bucket. Slighty modified version of this, now also handling already exisiting buckets:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html#create-an-amazon-s3-bucket

    :param s3_client: The S3 client connection
    :param bucket_name: Bucket to create
    :return: True if bucket created, else False 
    """
    try:
        logging.info(f"Creating bucket: {bucket}.")
        s3_client.create_bucket(Bucket=bucket)
        logging.info(f"Bucket '{bucket}' created.")
    except (ClientError, BotoCoreError) as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logging.info(f"Bucket '{bucket}' already exists.")
        else:
            logging.exception(f"Unexpected error creating bucket '{bucket}'.")
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

    for attempt in range(1, int(os.getenv('DEFAULT_RETRIES')) + 1):
        try:
            fileobj.seek(0)
            s3_client.upload_fileobj(fileobj, bucket, key)
            logging.info(f"Uploaded {key} to {bucket}.")
            return True
            
        except HTTPError as e:
            logging.error(f"HTTP error uploading {key} to {bucket}: {e}")
            if attempt == int(os.getenv('DEFAULT_RETRIES')):
                logging.error(f"All {os.getenv('DEFAULT_RETRIES')} attempts failed for {key}. Skipping.")
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

def delete_items(s3_client, bucket, prefix=""):
    """
    Deletes all objects in the specified S3 bucket and prefix.
    :param s3_client: Boto3 S3 client
    :param bucket: The S3 bucket name
    :param prefix: The prefix path inside the bucket
    :return: True if deletion was successful, False otherwise
    """
    logging.info(f"Preparing to delete all objects in sub-bucket {bucket}/{prefix}")
    try:
        # list objects to delete
        objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in objects_to_delete:
            logging.warning(f"No objects found with prefix '{prefix}'. Nothing to delete.")
            return True
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}

        # delete them
        response = s3_client.delete_objects(Bucket=bucket, Delete=delete_keys)

        if 'Errors' in response:
            logging.error("An error occurred during bulk delete.")
            for error in response['Errors']:
                logging.error(f" - Could not delete '{error['Key']}': {error['Message']}")
            return False

        logging.info(f"Successfully deleted {len(delete_keys['Objects'])} objects from '{prefix}'.")
        return True
    except ClientError as e:
        logging.error(f"A Boto3 client error occurred: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False