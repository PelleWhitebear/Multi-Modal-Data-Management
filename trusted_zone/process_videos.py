import logging
import boto3
import ffmpeg
import tempfile
from botocore.exceptions import ClientError

import dotenv
import os

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True 
)

TARGET_WIDTH = 1280
TARGET_HEIGHT = 720
TARGET_FPS = 30

def delete_videos(s3_client):
    bucket = os.getenv('TRUSTED_ZONE_BUCKET')
    prefix = "media/video/"
    logging.info(f"Preparing to delete all objects in sub-bucket {bucket}/{prefix}")
    try:
        # list objects to delete
        objects_to_delete = s3_client.list_objects_v2(Bucket=os.getenv('TRUSTED_ZONE_BUCKET'), Prefix=prefix)
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


def process_videos(s3_client, formatted_zone_prefix, trusted_zone_prefix):
    try:
        # List objects in the formatted zone
        objects = s3_client.list_objects_v2(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Prefix=formatted_zone_prefix)
        if 'Contents' not in objects:
            logging.info(f"No videos found in {formatted_zone_prefix}.")
            return

        for obj in objects['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue

            with tempfile.NamedTemporaryFile(suffix='.mp4') as temp_in, \
                 tempfile.NamedTemporaryFile(suffix='.mp4') as temp_out:
                
                logging.info(f'Processing video {key}')
                try:
                    # Download file from MinIO to temp file
                    s3_client.download_file(
                        Bucket=os.getenv("FORMATTED_ZONE_BUCKET"),
                        Key=key,
                        Filename=temp_in.name
                    )

                    # Corruption check
                    _ = ffmpeg.probe(temp_in.name)
                    logging.info(f"Successfully checked {key}. Applying transformations...")

                    # Create input stream
                    stream = ffmpeg.input(temp_in.name)

                    # Standardize FPS
                    stream = ffmpeg.filter(stream, 'fps', fps=TARGET_FPS, round='up')

                    # Standardize resolution
                    stream = ffmpeg.filter(
                        stream,
                        'scale',
                        width=TARGET_WIDTH,
                        height=TARGET_HEIGHT,
                        force_original_aspect_ratio='decrease'
                    )
                    stream = ffmpeg.filter(
                        stream,
                        'pad',
                        width=TARGET_WIDTH,
                        height=TARGET_HEIGHT,
                        x='(ow-iw)/2',  
                        y='(oh-ih)/2'
                    )

                    # Define output
                    stream = ffmpeg.output(stream, temp_out.name, acodec='copy')

                    # Run the process
                    ffmpeg.run(stream, overwrite_output=True, quiet=True)
                    logging.info(f"Successfully standardized video: {key}")

                    # Define new key for trusted zone
                    base_name = key.split('/')[-1]
                    new_key = f"{trusted_zone_prefix}{base_name}"

                    # Upload to trusted zone
                    s3_client.upload_file(
                        Filename=temp_out.name, 
                        Bucket=os.getenv("TRUSTED_ZONE_BUCKET"),
                        Key=new_key
                    )
                    logging.info(f"Successfully processed and uploaded: {new_key}")
                
                except ffmpeg.Error as e:
                    logging.warning(f"Failed to process video {key}. File is corrupted. Skipping. {e}")
                except ClientError as e:
                    logging.error(f"Boto3 error processing video {key}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error processing video {key}: {e}")

    except ClientError as e:
        logging.critical(f"Boto3 error listing videos in {formatted_zone_prefix}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"Unexpected error in process_videos: {e}", exc_info=True)


def main():
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        logging.info("Connected to MinIO.")
        delete_videos(s3_client)
        process_videos(
            s3_client,
            formatted_zone_prefix="media/video/",
            trusted_zone_prefix="media/video/"
        )
        logging.info("Video processing completed.")
    except ClientError as e:
        logging.error(f"A Boto3 error occurred: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()