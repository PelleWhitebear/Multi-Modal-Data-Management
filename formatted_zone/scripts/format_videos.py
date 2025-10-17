import sys
import os
import io
import boto3
import logging
import tempfile
from moviepy.editor import VideoFileClip
from botocore.exceptions import ClientError

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../global_scripts'))
sys.path.append(parent_dir)
from utils import *
from consts import *

setup_logging("format_videos.log")


def delete_videos_from_formatted(s3_client):
    logging.info(f"Preparing to delete all objects in sub-bucket {FORMATTED_ZONE_BUCKET}/{MEDIA_SUB_BUCKET}/video")
    prefix_videos = f"{MEDIA_SUB_BUCKET}/video/"
    try:
        # list objects to delete
        objects_to_delete = s3_client.list_objects_v2(Bucket=FORMATTED_ZONE_BUCKET, Prefix=prefix_videos)
        if 'Contents' not in objects_to_delete:
            logging.warning(f"No objects found with prefix '{prefix_videos}'. Nothing to delete.")
            return True
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}

        # delete them
        response = s3_client.delete_objects(Bucket=FORMATTED_ZONE_BUCKET, Delete=delete_keys)

        if 'Errors' in response:
            logging.error("An error occurred during bulk delete.")
            for error in response['Errors']:
                logging.error(f" - Could not delete '{error['Key']}': {error['Message']}")
            return False

        logging.info(f"Successfully deleted {len(delete_keys['Objects'])} objects from '{prefix_videos}'.")
        return True
    except ClientError as e:
        logging.error(f"A Boto3 client error occurred: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False


def format_video(s3_client, video_name):
    input_video_file = tempfile.NamedTemporaryFile(delete=False, suffix=".tmp")
    output_video_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{TARGET_VIDEO_FORMAT}")

    try:
        # download the original video from MinIO to the input temp file
        logging.info(f"Downloading '{video_name}' for formatting...")
        s3_client.download_file(LANDING_ZONE_BUCKET, video_name, input_video_file.name)
        logging.info(f"Successfully downloaded '{video_name}'.")

        # convert the video using moviepy
        logging.info(f"Converting '{video_name}' to {TARGET_VIDEO_FORMAT}...")
        with VideoFileClip(input_video_file.name) as video_clip:
            video_clip.write_videofile(output_video_file.name, codec='libx264', logger='bar')
        logging.info(f"Successfully converted '{video_name}'.")

        # 3. Upload the converted video to the formatted zone
        base_name = video_name.split('/')[-1].split('.')[0]
        new_key = f'{MEDIA_SUB_BUCKET}/video/{base_name}.{TARGET_VIDEO_FORMAT}'
        
        logging.info(f"Uploading formatted video to '{new_key}'...")
        s3_client.upload_file(output_video_file.name, FORMATTED_ZONE_BUCKET, new_key)
        logging.info(f"Successfully uploaded '{new_key}'.")
        return True

    except ClientError as e:
        logging.error(f"A Boto3 client error occurred processing '{video_name}': {e.response['Error']['Message']}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred processing '{video_name}': {e}")
        return False
    finally:
        # close and delete the temporary files
        input_video_file.close()
        output_video_file.close()
        os.remove(input_video_file.name)
        os.remove(output_video_file.name)
        logging.debug("Temporary files have been cleaned up.")


def move_to_formatted_zone(s3_client, video_name):
    try:
        base_name = video_name.split('/')[-1]
        new_key = f'{MEDIA_SUB_BUCKET}/video/{base_name}'

        s3_client.copy_object(
            Bucket=FORMATTED_ZONE_BUCKET,
            CopySource={
                "Bucket": LANDING_ZONE_BUCKET,
                "Key": video_name
            },
            Key=new_key
        )
        logging.info(f"Successfully copied '{video_name}' to '{new_key}' in the formatted zone.")
        return True
    except ClientError as e:
        # Handle specific Boto3/S3 errors
        logging.error(f"A Boto3 client error occurred while copying '{video_name}': {e.response['Error']['Message']}")
        return False
    except Exception as e:
        # Handle other unexpected errors
        logging.error(f"An unexpected error occurred while copying '{video_name}': {e}")
        return False


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
        
        # delete videos in the formatted zone to update them
        success = delete_videos_from_formatted(s3_client)
        if success:
            # retrieve videos from landing zone
            objects = s3_client.list_objects_v2(Bucket=LANDING_ZONE_BUCKET, Prefix=f'{PERSISTENT_SUB_BUCKET}/media/video/')

            logging.info("Starting video format transformation...")
            for vid in objects['Contents']:
                vid_name = vid['Key']
                format_vid = vid_name.split('.')[-1].lower()
                # if the video format is different from MP4, is formatted and moved to formatted zone
                if format_vid != TARGET_VIDEO_FORMAT:
                    format_video(s3_client, vid_name)
                # otherwise, it is moved directly to the formatted zone
                else:
                    move_to_formatted_zone(s3_client, vid_name)
            logging.info('Video formatting completed.')

    except Exception as e:
        logging.error(f"Error connecting to MinIO: {e}")
        return


if __name__ == '__main__':
    main()