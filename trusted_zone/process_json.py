import json
import logging
from botocore.exceptions import ClientError
import dotenv
import os
import numbers
from global_scripts.utils import minio_init

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True 
)

STEAM_REQUIRED_KEYS = [
    "name", "release_date", "required_age", "price", "dlc_count",
    "detailed_description", "about_the_game", "header_image", "support_url",
    "support_email", "windows", "mac", "linux", "metacritic_score",
    "metacritic_url", "achievements", "recommendations", "notes",
    "supported_languages", "full_audio_languages", "packages", "developers",
    "publishers", "categories", "genres", "screenshots", "movies"
]
STEAMSPY_REQUIRED_KEYS = [
    "user_score", "score_rank", "positive", "negative", "estimated_owners",
    "average_playtime_forever", "average_playtime_2weeks",
    "median_playtime_forever", "median_playtime_2weeks", "discount",
    "peak_ccu", "tags"
]
NON_NEGATIVE_STEAM_FIELDS = ["required_age", "price", "dlc_count", "recommendations"]
EXPECTED_LIST_FIELDS = [
    "supported_languages", "full_audio_languages", "packages", "developers",
    "publishers", "categories", "genres", "screenshots", "movies"
]
EXPECTED_BOOL_FIELDS = ["windows", "mac", "linux"]
EXPECTED_INT_FIELDS_STEAM = ["required_age", "dlc_count", "metacritic_score", "achievements", "recommendations"]
EXPECTED_INT_FIELDS_STEAMSPY = [
    "user_score", "positive", "negative", "average_playtime_forever", "average_playtime_2weeks", 
    "median_playtime_forever", "median_playtime_2weeks", "peak_ccu"
]
EXPECTED_NUMERIC_FIELDS_STEAM = ["price"]
EXPECTED_DICT_FIELDS_STEAMSPY = ["tags"]


def validate_and_clean_entry(game_id, game_data, required_keys, dataset_name):
    """
    Validates and cleans a single game entry.

    :param game_id: The ID of the game
    :param game_data: The dictionary containing game data
    :param required_keys: List of required keys for validation
    :param dataset_name: Name of the dataset (e.g., "Steam" or "SteamSpy")
    :return: Cleaned game data dictionary if valid, else None
    """
    missing_keys = [key for key in required_keys if key not in game_data]
    if missing_keys:
        logging.warning(f"[{dataset_name} ID: {game_id}] Missing required keys: {missing_keys}. Skipping entry.")
        return None

    cleaned_data = game_data.copy()

    # type validation
    expected_int_fields = EXPECTED_INT_FIELDS_STEAM if dataset_name == "Steam" else EXPECTED_INT_FIELDS_STEAMSPY
    expected_numeric_fields = EXPECTED_NUMERIC_FIELDS_STEAM if dataset_name == "Steam" else []
    expected_dict_fields = EXPECTED_DICT_FIELDS_STEAMSPY if dataset_name == "SteamSpy" else []

    # check integers
    for field in expected_int_fields:
        value = cleaned_data.get(field)
        if value is not None and not isinstance(value, int):
            try:
                cleaned_data[field] = int(value)
            except (ValueError, TypeError):
                 logging.warning(f"[{dataset_name} ID: {game_id}] Field '{field}' has non-integer value '{value}'. Skipping entry.")
                 return None

    # check numerics
    for field in expected_numeric_fields:
         value = cleaned_data.get(field)
         if value is not None and not isinstance(value, numbers.Number):
             try:
                 cleaned_data[field] = float(value)
             except (ValueError, TypeError):
                  logging.warning(f"[{dataset_name} ID: {game_id}] Field '{field}' has non-numeric value '{value}'. Skipping entry.")
                  return None

    # check Booleans
    for field in EXPECTED_BOOL_FIELDS:
        if field in cleaned_data and not isinstance(cleaned_data.get(field), bool):
            logging.warning(f"[{dataset_name} ID: {game_id}] Field '{field}' has non-boolean value '{cleaned_data.get(field)}'. Skipping entry.")
            return None 

    # check that Lists are Lists
    for field in EXPECTED_LIST_FIELDS:
        if field in cleaned_data:
            value = cleaned_data.get(field)
            if value is None:
                cleaned_data[field] = [] 
            elif not isinstance(value, list):
                logging.warning(f"[{dataset_name} ID: {game_id}] Field '{field}' expected list, got {type(value).__name__}. Skipping entry.")
                return None

    # check that Dicts are Dicts
    for field in expected_dict_fields:
         if field in cleaned_data:
            value = cleaned_data.get(field)
            if value is None:
                cleaned_data[field] = {} 
            elif not isinstance(value, dict):
                 logging.warning(f"[{dataset_name} ID: {game_id}] Field '{field}' expected dict, got {type(value).__name__}. Skipping entry.")
                 return None

    # check non negative
    if dataset_name == "Steam":
        for field in NON_NEGATIVE_STEAM_FIELDS:
            value = cleaned_data.get(field)
            if isinstance(value, numbers.Number) and value < 0:
                logging.warning(f"[{dataset_name} ID: {game_id}] Field '{field}' has negative value {value}. Correcting to 0.")
                cleaned_data[field] = 0 

    return cleaned_data


def process_json_trusted(s3_client, formatted_zone_path, trusted_zone_path, required_keys, dataset_name):
    """
    Processes JSON files from the formatted zone, validates and cleans the data,
    and uploads the cleaned data to the trusted zone.
    
    :param s3_client: Boto3 S3 client
    :param formatted_zone_path: Prefix for the formatted zone
    :param trusted_zone_path: Prefix for the trusted zone
    :param required_keys: List of required keys for validation
    :param dataset_name: Name of the dataset (e.g., "Steam" or "SteamSpy")
    """
    processed_data = {}
    invalid_entry_count = 0
    total_entries_read = 0

    try:
        objects = s3_client.list_objects_v2(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Prefix=formatted_zone_path)
        if 'Contents' not in objects or not objects['Contents']:
            logging.info(f"No files found in {formatted_zone_path}.")
            return

        file_key = None
        for obj in objects['Contents']:
            if not obj['Key'].endswith('/') and obj['Key'].lower().endswith('.json'):
                file_key = obj['Key']
                break 

        if not file_key:
             logging.warning(f"No .json files found directly under {formatted_zone_path}")
             return

        logging.info(f"Processing file: {file_key}")
        try:
            response = s3_client.get_object(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Key=file_key)
            file_content = response['Body'].read().decode('utf-8')
            raw_data = json.loads(file_content)
        except (ClientError, json.JSONDecodeError, Exception) as e:
            logging.error(f"Failed to load or parse JSON from {file_key}: {e}. Skipping file.")
            return

        if not isinstance(raw_data, dict):
             logging.error(f"Expected JSON to be a dictionary (object), but got {type(raw_data).__name__}. Skipping file {file_key}.")
             return

        total_entries_read = len(raw_data)
        logging.info(f"Read {total_entries_read} entries from {file_key}.")

        for game_id, game_data in raw_data.items():
            if not isinstance(game_data, dict):
                logging.warning(f"[{dataset_name} ID: {game_id}] Entry data is not a dictionary. Skipping.")
                invalid_entry_count += 1
                continue

            cleaned_entry = validate_and_clean_entry(game_id, game_data, required_keys, dataset_name)

            if cleaned_entry is not None:
                processed_data[game_id] = cleaned_entry
            else:
                invalid_entry_count += 1

        logging.info(f"Validation complete for {file_key}: {len(processed_data)} valid entries, {invalid_entry_count} invalid entries skipped.")

        if not processed_data:
             logging.warning(f"No valid data remaining for {file_key} after cleaning. Nothing to upload.")
             return

        standardized_data_str = json.dumps(processed_data, indent=4, sort_keys=True)

        base_name = file_key.split('/')[-1]
        if not trusted_zone_path.endswith('/'):
            trusted_zone_path += '/'
        new_key = f"{trusted_zone_path}{base_name}"

        s3_client.put_object(
            Bucket=os.getenv("TRUSTED_ZONE_BUCKET"),
            Key=new_key,
            Body=standardized_data_str.encode('utf-8'),
            ContentType='application/json' 
        )
        logging.info(f"Successfully processed and uploaded cleaned data to: {new_key}")

    except ClientError as e:
        logging.critical(f"Boto3 error during processing for {formatted_zone_path}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"Unexpected error in process_json_trusted for {formatted_zone_path}: {e}", exc_info=True)


def main():
    s3_client = minio_init()

    # process Steam API JSON files
    logging.info("Starting Steam JSON Processing...")
    process_json_trusted(
        s3_client,
        formatted_zone_path="json/steam/", 
        trusted_zone_path="json/steam/", 
        required_keys=STEAM_REQUIRED_KEYS,
        dataset_name="Steam"
    )

    # process SteamSpy API JSON files
    logging.info("Starting SteamSpy JSON Processing...")
    process_json_trusted(
        s3_client,
        formatted_zone_path="json/steamspy/",
        trusted_zone_path="json/steamspy/",
        required_keys=STEAMSPY_REQUIRED_KEYS,
        dataset_name="SteamSpy"
    )

    logging.info("JSON Processing Completed")


if __name__ == "__main__":
    main()