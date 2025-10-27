import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
import urllib3
from tqdm import tqdm
import io
import dotenv
from global_scripts.utils import minio_init, ingest_data, load_games_from_minio

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

def upload_file(s3_client, url, key):
    """
    Upload a single media file (image or video) to the temporal sub-bucket.

    :param s3_client: The S3 client connection
    :param url: The URL of the media file to upload
    :return: True if upload succeeded, else False
    """
    timeout = float(os.getenv("DEFAULT_TIMEOUT"))
    sleep = float(os.getenv("DEFAULT_SLEEP"))
    for attempt in range(1, int(os.getenv("DEFAULT_RETRIES")) + 1):

        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            if int(response.headers.get("Content-Length", 0)) == 0:
                logging.warning(f"Skipping empty response from {url}")
                return False
            key = f"{os.getenv('TEMPORAL_SUB_BUCKET')}/{key}"
            fileobj = io.BytesIO(response.content)
            return ingest_data(s3_client, os.getenv('LANDING_ZONE_BUCKET'), fileobj, key)


        # Exponential backoff for retries
        except (requests.RequestException, urllib3.exceptions.ReadTimeoutError) as e:
            logging.warning(f"Attempt {attempt} failed: {e}")
            timeout *= 2
            sleep *= 2
            if attempt == int(os.getenv("DEFAULT_RETRIES")):
                logging.error(f"All {int(os.getenv('DEFAULT_RETRIES'))} attempts failed for {url}. Skipping.")
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
    """
    try:
        # Upload each file concurrently
        with ThreadPoolExecutor(max_workers=int(os.getenv("MAX_THREADS"))) as executor:
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
    Gets steam_games.json from the landing zone bucket, extracts 5 image URLs and 1 video URL. 
    Returns a dictionary with the results.

    :param s3_client: The S3 client connection
    :return: Dictionary with media URLs
    """
    media = {}

    try:
        games = load_games_from_minio(s3_client, os.getenv("LANDING_ZONE_BUCKET"), os.getenv("TEMPORAL_SUB_BUCKET"), "steam_games.json")
        images = 0
        videos = 0

        for game_id, game_info in games.items():
            # Python supports [:5] even if there are less than 5 items in the list.
            new_images = game_info.get("screenshots", [])[:5]
            new_videos = game_info.get("movies", None)[0] if game_info.get("movies", None) else None

            if len(new_images) != 5:
                logging.warning(f"Game ID {game_id} does not have 5 images, but {len(new_images)}.")
            if not new_videos:
                logging.warning(f"Game ID {game_id} does not have a video.")

            images += len(new_images)
            videos += 1 if new_videos else 0

            media[game_id] = {
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
    s3_client = minio_init()

    # Get media URLs from steam_games.json
    media = get_media_urls(s3_client)
            
    # Upload media files concurrently        
    if media:
        upload_concurrently(s3_client, media)

if __name__ == "__main__":
    main()