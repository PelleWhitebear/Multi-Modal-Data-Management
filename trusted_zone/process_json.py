import json
import boto3
import logging
from botocore.exceptions import ClientError
import dotenv
import os

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

############################################################

# Here since it is only used in this script
STEAM_REQUIRED_KEYS = [
        # "id",
        "name",
        "release_date",
        "required_age",
        "price",
        "dlc_count",
        "detailed_description", 
        "about_the_game",
        "header_image",
        "support_url",
        "support_email",
        "windows",
        "mac",
        "linux",
        "metacritic_score",
        "metacritic_url",
        "achievements",
        "recommendations",
        "notes",
        "supported_languages",
        "full_audio_languages",
        "packages",
        "developers",
        "publishers",
        "categories",
        "genres",
        "screenshots",
        "movies"
]

STEAMSPY_REQUIRED_KEYS = [
        "user_score",
        "score_rank",
        "positive",
        "negative",
        "estimated_owners",
        "average_playtime_forever",
        "average_playtime_2weeks",
        "median_playtime_forever",
        "median_playtime_2weeks",
        "discount",
        "peak_ccu",
        "tags"
]

############################################################

def validate_json_structure(json_data, required_keys):
    if isinstance(json_data, dict):
        # If all values are dicts (Steam/SteamSpy format), validate each
        if all(isinstance(v, dict) for v in json_data.values()):
            for k, v in json_data.items():
                missing = [key for key in required_keys if key not in v]
                if missing:
                    logging.debug(f"Entry {k} missing keys: {missing}")
                    return False
            return True
        else:
            present = set(json_data.keys())
            missing = [key for key in required_keys if key not in json_data]
            logging.debug(f"Top-level keys present: {present}, missing: {missing}")
            return not missing
    elif isinstance(json_data, list):
        if not json_data:
            logging.debug("JSON list is empty.")
            return False
        present = set(json_data[0].keys()) if isinstance(json_data[0], dict) else set()
        missing = [key for key in required_keys if key not in present]
        logging.debug(f"First item keys present: {present}, missing: {missing}")
        for i, item in enumerate(json_data):
            if not isinstance(item, dict):
                logging.debug(f"Item {i} is not a dict: {item}")
                return False
            if not all(key in item for key in required_keys):
                logging.debug(f"Item {i} missing required keys.")
                return False
        return True
    else:
        logging.debug(f"JSON data is neither dict nor list: {type(json_data)}")
        return False

def process_json(s3_client, formatted_zone_path, trusted_zone_path, required_keys):
    try:
        # List objects in the formatted zone
        objects = s3_client.list_objects_v2(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Prefix=formatted_zone_path)
        if 'Contents' not in objects:
            logging.info(f"No files found in {formatted_zone_path}.")
            return

        for obj in objects['Contents']:
            key = obj['Key']
            logging.info(f"Processing file: {key}")

            try:
                # Get the object content
                response = s3_client.get_object(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Key=key)
                file_content = response['Body'].read().decode('utf-8')

                try:
                    data = json.loads(file_content)
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse JSON for file {key}: {e}")
                    continue

                # Log the parsed JSON content for debugging
                logging.debug(f"Parsed JSON content of {key}: {data}")

                # Validate JSON structure
                if not validate_json_structure(data, required_keys):
                    logging.warning(f"Skipping invalid JSON file: {key}")
                    continue

                # Standardize the JSON formatting
                standardized_data = json.dumps(data, indent=4, sort_keys=True)

                # Define the new key for the trusted zone
                base_name = key.split('/')[-1]
                new_key = f"{trusted_zone_path}/{base_name}"

                # Upload the standardized JSON to the trusted zone
                s3_client.put_object(
                    Bucket=os.getenv("TRUSTED_ZONE_BUCKET"),
                    Key=new_key,
                    Body=standardized_data.encode('utf-8')
                )
                logging.info(f"Successfully processed and uploaded: {new_key}")

            except ClientError as e:
                logging.error(f"Boto3 error processing file {key}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing file {key}: {e}")

    except ClientError as e:
        logging.critical(f"Boto3 error listing objects in {formatted_zone_path}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"Unexpected error in process_json: {e}", exc_info=True)

def main():
    try:

        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        logging.info("Connected to MinIO.")

        # Process Steam API JSON files
        process_json(
            s3_client,
            formatted_zone_path="json/steam",
            trusted_zone_path="json/steam",
            required_keys=STEAM_REQUIRED_KEYS
        )

        # Process SteamSpy API JSON files
        process_json(
            s3_client,
            formatted_zone_path="json/steamspy",
            trusted_zone_path="json/steamspy",
            required_keys=STEAMSPY_REQUIRED_KEYS
        )

        logging.info("Processing completed.")

    except ClientError as e:
        logging.error(f"A Boto3 error occurred: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()