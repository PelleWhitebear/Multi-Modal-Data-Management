import boto3
import logging
from utils import *
from consts import *
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
import json
import urllib3
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - [%(levelname)s] - %(message)s', 
                    filename='landing_zone/logs/ingest_media.log', 
                    force=True,
                    filemode='w')  # Overwrite log file on each run

def upload_file(s3_client, url, key):
    """
    Upload a single media file (image or video) to the temporal sub-bucket.

    :param s3_client: The S3 client connection
    :param url: The URL of the media file to upload
    :return: True if upload succeeded, else False
    """
    timeout = args.timeout
    sleep = args.sleep
    for attempt in range(1, args.retries + 1):
        
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            if int(response.headers.get("Content-Length", 0)) == 0:
                logging.warning(f"Skipping empty response from {url}")
                return False
            # Exception handling is done inside ingest_data
            key = f"{TEMPORAL_SUB_BUCKET}/{key}"
            fileobj = io.BytesIO(response.content)
            return ingest_data(s3_client, LANDING_ZONE_BUCKET, fileobj, key)


        except (requests.RequestException, urllib3.exceptions.ReadTimeoutError) as e:
            logging.warning(f"Attempt {attempt} failed: {e}")
            timeout *= 2
            sleep *= 2
            if attempt == args.retries:
                logging.error(f"All {args.retries} attempts failed for {url}. Skipping.")
                return False
            time.sleep(sleep)
        
        except Exception:
            logging.exception(f"Unexpected error downloading {url}. Skipping.")
            return False

def upload_concurrently(s3_client, media):
    """
    Upload all media files (images and videos) concurrently to the temporal sub-bucket.

    :param s3_client: The S3 client connection
    :param media: Dictionary with media URLs
    :return: True if no errors occurred, else False
    """
    try:
        # Upload each file concurrently
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for game_id, game_info in media.items():
                for image_idx, image_file in enumerate(game_info.get("images", []), start=1):
                    if image_file:
                        ext = image_file.split('/')[-1].split('?')[0].split('.')[-1]
                        key = f"{game_id}_{image_idx}.{ext}"
                        futures.append(executor.submit(upload_file, s3_client, image_file, key))

                for video_idx, video_file in enumerate([game_info.get("video", None)], start=1):
                    if video_file:
                        ext = video_file.split('/')[-1].split('?')[0].split('.')[-1]
                        key = f"{game_id}_{video_idx}.{ext}"
                        futures.append(executor.submit(upload_file, s3_client, video_file, key))

            # Wait for all uploads to complete
            fail = 0
            for future in tqdm(as_completed(futures), total=len(futures)):
                if future.result() is False:                    
                    fail = 1

        return True if fail == 0 else False

    except Exception:
        logging.exception(f"Error uploading media files.")
        return False

def get_media_urls(s3_client):
    """
    Gets `steam_games.json` from the landing zone bucket, extracts 5 image URLs and 1 video URL. 
    Returns a dictionary with the results.

    :param s3_client: The S3 client connection
    :return: Dictionary with media URLs
    """
    media = {}

    try:
        s3_response = s3_client.get_object(Bucket=LANDING_ZONE_BUCKET, Key=f"{TEMPORAL_SUB_BUCKET}/steam_games.json")
        games = json.loads(s3_response["Body"].read().decode("utf-8"))
        images = 0
        videos = 0
        count = 0
        for game_id, game_info in games.items():
            count += 1
            new_images = game_info.get("screenshots", [])[:5]
            new_videos = game_info.get("movies", None)[0] if game_info.get("movies", None) else None

            if len(new_images) != 5:
                logging.warning(f"Game ID {game_id} does not have 5 images, but {len(new_images)}.")
            if not new_videos:
                logging.warning(f"Game ID {game_id} does not have a video.")

            images += len(new_images)
            videos += 1 if new_videos else 0

            media[game_id] = {
                # Python supports [:5] even if there are less than 5 items in the list.
                "images": new_images,
                "video": new_videos
            }
        
        logging.info(f"Media extraction completed. {len(games)} games processed.")
        logging.info(f"Total images found: {images}")
        logging.info(f"Total videos found: {videos}")

        return media

    except Exception:
        logging.exception(f"Error retrieving or processing steam_games.json.")
        return media

def main():

    # MinIO client connection, using Amazon S3 API and boto3 Python library
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        logging.info("Connected to MinIO.")

        try:
            media = get_media_urls(s3_client)
            
            if media:
                upload_concurrently(s3_client, media)

        except Exception as e:
            logging.error(f"Error during data ingestion: {e}")
            return

    except Exception as e:
        logging.error(f"Error connecting to MinIO: {e}")
        return
    

if __name__ == "__main__":
    logging.info(f'Starting Steam games scraper.')
    parser = argparse.ArgumentParser(description='Steam games scraper.')
    parser.add_argument('-s', '--sleep',    type=float, default=DEFAULT_SLEEP,    help='Waiting time between requests')
    parser.add_argument('-t', '--timeout',  type=float, default=DEFAULT_TIMEOUT,  help='Timeout for each request')
    parser.add_argument('-r', '--retries',  type=int,   default=DEFAULT_RETRIES,  help='Number of retries (0 to always retry)')
    args = parser.parse_args()

    if 'h' in args or 'help' in args:
        parser.print_help()
        sys.exit()

    main()