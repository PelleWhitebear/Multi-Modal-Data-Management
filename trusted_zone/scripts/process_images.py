import logging
import boto3
from botocore.exceptions import ClientError
from io import BytesIO
from PIL import Image, ImageEnhance
import dotenv
import os
from global_scripts.utils import create_bucket, create_sub_bucket

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

def process_images(s3_client, formatted_zone_prefix, trusted_zone_prefix):
    try:
        # List objects in the formatted zone
        objects = s3_client.list_objects_v2(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Prefix=formatted_zone_prefix)
        if 'Contents' not in objects:
            logging.info(f"No images found in {formatted_zone_prefix}.")
            return

        for obj in objects['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue
            logging.info(f"Processing image: {key}")
            try:
                # Get the image from MinIO
                response = s3_client.get_object(Bucket=os.getenv("FORMATTED_ZONE_BUCKET"), Key=key)
                file_content = response['Body'].read()
                img = Image.open(BytesIO(file_content))
                img = img.convert('RGB')
                img = img.resize((256, 256))
                enhancer = ImageEnhance.Brightness(img)
                img = enhancer.enhance(1.0)
                # Save to buffer (removes metadata)
                buffer = BytesIO()
                img.save(buffer, format='JPEG')
                buffer.seek(0)
                # Define new key for trusted zone
                base_name = key.split('/')[-1]
                new_key = f"{trusted_zone_prefix}/{base_name}"
                # Upload to trusted zone
                s3_client.put_object(
                    Bucket=os.getenv("TRUSTED_ZONE_BUCKET"),
                    Key=new_key,
                    Body=buffer.getvalue(),
                    ContentType='image/jpeg'
                )
                logging.info(f"Successfully processed and uploaded: {new_key}")
            except ClientError as e:
                logging.error(f"Boto3 error processing image {key}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing image {key}: {e}")
    except ClientError as e:
        logging.critical(f"Boto3 error listing images in {formatted_zone_prefix}: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"Unexpected error in process_images: {e}", exc_info=True)

def main():
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        logging.info("Connected to MinIO.")
        process_images(
            s3_client,
            formatted_zone_prefix="media/image",
            trusted_zone_prefix="media/image"
        )
        logging.info("Image processing completed.")
    except ClientError as e:
        logging.error(f"A Boto3 error occurred: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()