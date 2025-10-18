import sys
import os
import io
import boto3
import logging
from PIL import Image
from botocore.exceptions import ClientError

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../global_scripts'))
sys.path.append(parent_dir)
from utils import *
from consts import *

setup_logging("format_images.log")


def delete_images_from_formatted(s3_client):
    logging.info(f"Preparing to delete all objects in sub-bucket {FORMATTED_ZONE_BUCKET}/{MEDIA_SUB_BUCKET}/image")
    prefix_images = f"{MEDIA_SUB_BUCKET}/image/"
    try:
        # list objects to delete
        objects_to_delete = s3_client.list_objects_v2(Bucket=FORMATTED_ZONE_BUCKET, Prefix=prefix_images)
        if 'Contents' not in objects_to_delete:
            logging.warning(f"No objects found with prefix '{prefix_images}'. Nothing to delete.")
            return True
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}

        # delete them
        response = s3_client.delete_objects(Bucket=FORMATTED_ZONE_BUCKET, Delete=delete_keys)

        if 'Errors' in response:
            logging.error("An error occurred during bulk delete.")
            for error in response['Errors']:
                logging.error(f" - Could not delete '{error['Key']}': {error['Message']}")
            return False

        logging.info(f"Successfully deleted {len(delete_keys['Objects'])} objects from '{prefix_images}'.")
        return True
    except ClientError as e:
        logging.error(f"A Boto3 client error occurred: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return False


def format_image(s3_client, img_name):
    try:
        # download img to memory
        resp = s3_client.get_object(
            Bucket=LANDING_ZONE_BUCKET,
            Key=img_name
        )
        image_data = resp['Body'].read()

        # open img with Pillow
        with Image.open(io.BytesIO(image_data)) as img:
            # Save converted img to JPG buffer
            output_buffer = io.BytesIO()
            img.save(output_buffer, format='JPEG')
            output_buffer.seek(0)

        # create new key for formatted image
        base_name = img_name.split('/')[-1].split('.')[0]
        new_key = f'{MEDIA_SUB_BUCKET}/image/{base_name}.{TARGET_IMG_FORMAT}'

        s3_client.upload_fileobj(output_buffer, FORMATTED_ZONE_BUCKET, new_key)
        logging.info(f"Successfully converted '{img_name}' to '{new_key}'")

    except Exception as e:
        logging.error(f"Failed to process image '{img_name}'. Error: {e}")


def move_to_formatted_zone(s3_client, img_name):
    try:
        base_name = img_name.split('/')[-1]
        new_key = f'{MEDIA_SUB_BUCKET}/image/{base_name}'

        s3_client.copy_object(
            Bucket=FORMATTED_ZONE_BUCKET,
            CopySource={
                "Bucket": LANDING_ZONE_BUCKET,
                "Key": img_name
            },
            Key=new_key
        )
        logging.info(f"Successfully copied '{img_name}' to '{new_key}' in the formatted zone.")
        return True
    except ClientError as e:
        # Handle specific Boto3/S3 errors
        logging.error(f"A Boto3 client error occurred while copying '{img_name}': {e.response['Error']['Message']}")
        return False
    except Exception as e:
        # Handle other unexpected errors
        logging.error(f"An unexpected error occurred while copying '{img_name}': {e}")
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
        
        # delete images in the formatted zone to update them
        success = delete_images_from_formatted(s3_client)
        if success:
            # retrieve images from landing zone
            objects = s3_client.list_objects_v2(Bucket=LANDING_ZONE_BUCKET, Prefix=f'{PERSISTENT_SUB_BUCKET}/media/image/')
            if not 'Contents' in objects or not objects['Contents']:
                logging.error('There are no images in the persistent zone.')

            logging.info("Starting image format transformation...")
            for img in objects['Contents']:
                name_img = img['Key']
                format_img = name_img.split('.')[-1].lower()
                # if the image format is different from JPG, is formatted and moved to formatted zone
                if format_img != TARGET_IMG_FORMAT:
                    format_image(s3_client, name_img)
                # otherwise, it is moved directly to the formatted zone
                else:
                    move_to_formatted_zone(s3_client, name_img)
            logging.info('Image formatting completed.')

    except Exception as e:
        logging.error(f"Error connecting to MinIO: {e}")
        return


if __name__ == '__main__':
    logging.info(f'Starting images formatting.')
    main()