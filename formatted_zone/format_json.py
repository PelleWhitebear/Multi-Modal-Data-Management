import sys
import os
import boto3
import logging
import json
import csv
import io
import xmltodict
import yaml
from botocore.exceptions import ClientError
import dotenv
import os

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)


def is_empty(s3_client, bucket_name, folder_prefix):
    try:
        objects = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_prefix,
            MaxKeys=2
        )

        if 'Contents' not in objects:
            logging.info(f"Folder '{folder_prefix}' is empty (or does not exist).")
            return True

        if len(objects['Contents']) == 1:
            first_key = objects['Contents'][0]['Key']
            if first_key == folder_prefix:
                logging.info(f"Folder '{folder_prefix}' is empty (contains only placeholder).")
                return True
            else:
                logging.info(f"Folder '{folder_prefix}' is not empty.")
                return False
            
        logging.info(f"Folder '{folder_prefix}' is NOT_EMPTY.")
        return False

    except ClientError as e:
        logging.error(f"Error checking folder {folder_prefix}: {e}")
        raise


def handle_csv(file_content_string):
    try:
        reader = csv.DictReader(io.StringIO(file_content_string))
        return list(reader)
    except Exception as e:
        logging.error(f"Failed to parse CSV: {e}")
        raise


def handle_xml(file_content_bytes):
    try:
        return xmltodict.parse(file_content_bytes)
    except Exception as e:
        logging.error(f"Failed to parse XML: {e}")
        raise


def handle_yaml(file_content_bytes):
    try:
        return yaml.safe_load(file_content_bytes)
    except Exception as e:
        logging.error(f"Failed to parse YAML: {e}")
        raise


def format_to_json(s3_client, source_key, type_folder):
    try:
        # get object content from landing zone
        obj = s3_client.get_object(Bucket=os.getenv("LANDING_ZONE_BUCKET"), Key=source_key)
        file_content = obj['Body'].read()
        
        file_format = source_key.split('.')[-1].lower()
        processed_data = None
        
        # apply different formatting based on file extension
        if file_format == 'csv':
            processed_data = handle_csv(file_content.decode('utf-8'))
        elif file_format == 'xml':
            processed_data = handle_xml(file_content)
        elif file_format in ('yaml', 'yml'):
            processed_data = handle_yaml(file_content)
        else:
            logging.warning(f"Unsupported format '{file_format}' for file {source_key}. Skipping.")
            return False

        if processed_data is None:
            logging.error(f"Failed to process data for {source_key}.")
            return False
            
        # convert the processed python dict to JSON bytes
        json_content_bytes = json.dumps(processed_data, indent=4).encode('utf-8')
        
        # create new key for formatted image
        base_name = source_key.split('/')[-1].split('.')[0]
        new_key = f"{os.getenv('JSON_SUB_BUCKET')}/{type_folder}/{base_name}.{os.getenv('TARGET_TAB_FORMAT')}"
        
        # upload the new JSON file to the formatted zone
        s3_client.put_object(
            Bucket=os.getenv("FORMATTED_ZONE_BUCKET"),
            Key=new_key,
            Body=json_content_bytes
        )
        logging.info(f"Successfully formatted '{source_key}' and uploaded to '{new_key}'.")
        return True
        
    except ClientError as e:
        logging.error(f"Boto3 error formatting file {source_key}: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error formatting file {source_key}: {e}")
        raise


def move_to_formatted_zone(s3_client, key, type):
    try:
        base_name = key.split('/')[-1]
        new_key = f"{os.getenv('JSON_SUB_BUCKET')}/{type}/{base_name}"

        s3_client.copy_object(
            Bucket=os.getenv("FORMATTED_ZONE_BUCKET"),
            CopySource={
                "Bucket": os.getenv("LANDING_ZONE_BUCKET"),
                "Key": key
            },
            Key=new_key
        )
        logging.info(f"Successfully copied '{key}' to '{new_key}' in the formatted zone.")
        return True
    except ClientError as e:
        # Handle specific Boto3/S3 errors
        logging.error(f"A Boto3 client error occurred while copying '{key}': {e.response['Error']['Message']}")
        return False
    except Exception as e:
        # Handle other unexpected errors
        logging.error(f"An unexpected error occurred while copying '{key}': {e}")
        return False


def format_json_objects(s3_client, objects, type):
    if not 'Contents' in objects or not objects['Contents']:
        logging.error(f'No {type} files found.')
        return
    
    # get last file uploaded. Returned objets are ordered alphabetically, 
    # so the last item in the list is the most recent file
    last_key = objects['Contents'][-1]['Key'] 
    logging.info(f"Most recent {type} file: {last_key}")

    # if the formatted zone is empty, apply formatting if needed and move file to formatted
    if is_empty(s3_client, os.getenv("FORMATTED_ZONE_BUCKET"), f"{os.getenv('JSON_SUB_BUCKET')}/{type}/"):
        curr_format = last_key.split('.')[-1].lower()
        if curr_format != os.getenv("TARGET_TAB_FORMAT"):
            format_to_json(s3_client, last_key, type)
        else:
            move_to_formatted_zone(s3_client, last_key, type)

    # otherwise, check if the date of files in landing and formatted is the same
    else:
        try:
            prefix = f"{os.getenv('JSON_SUB_BUCKET')}/{type}/"
            formatted_zone_objects  = s3_client.list_objects_v2(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Prefix=prefix)
            old_key = formatted_zone_objects['Contents'][0]['Key'] # should be only one file in json sub-bucket in formatted zone
            logging.info(f"Most recent file in formatted zone: {old_key}")

            formatted_file_date = old_key.split('#')[1]
            landing_file_date = last_key.split('#')[1]

            # if the date is the same, nothing needs to be updated:
            if formatted_file_date == landing_file_date:
                logging.info(f"File {last_key} is already up to date in formatted zone. Skipping.")
                return
            
            # otherwise, update it:
            if formatted_file_date != landing_file_date:
                # delete old file in formatted zone
                logging.info(f"New file found. Deleting {old_key} and replacing with {last_key}.")
                s3_client.delete_object(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Key=old_key)

                # apply formatting if needed and move files to formatted
                curr_format = last_key.split('.')[-1]
                if curr_format != os.getenv("TARGET_TAB_FORMAT"):
                    format_to_json(s3_client, last_key, type)
                else:
                    move_to_formatted_zone(s3_client, last_key, type)

        except ClientError as e:
            logging.critical(f"CRITICAL: Boto3 error during update for {type}. Failed to delete {old_key} or list objects. Error: {e}", exc_info=True)
            raise


def main():
     # MinIO client connection, using Amazon S3 API and boto3 Python library
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        logging.info("Connected to MinIO.")

        # retrieve steam and steamspy objects and format them
        logging.info('Formatting started...')
        json_path = f"{os.getenv('PERSISTENT_SUB_BUCKET')}/json/"
        steam_objects = s3_client.list_objects_v2(Bucket=os.getenv("LANDING_ZONE_BUCKET"), Prefix=json_path+'steam/')
        steamspy_objects = s3_client.list_objects_v2(Bucket=os.getenv("LANDING_ZONE_BUCKET"), Prefix=json_path+'steamspy/')

        format_json_objects(s3_client, steam_objects, 'steam')
        format_json_objects(s3_client, steamspy_objects, 'steamspy')
        logging.info('Formatting completed.')

    except ClientError as e:
        logging.critical(f"A Boto3 error occurred: {e}", exc_info=True)
        return
    except Exception as e:
        logging.critical(f"An unexpected error occurred in main: {e}", exc_info=True)
        return


if __name__ == '__main__':
    main()