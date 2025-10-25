import os
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True  # override any existing config
)

def delete_media(s3_client, bucket, prefix):
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

        # delete old images (we assume images are not updated so we delete old to insert new ones)
        del_img = delete_media(s3_client=s3_client, bucket=os.getenv("LANDING_ZONE_BUCKET"), 
                               prefix=f"{os.getenv('PERSISTENT_SUB_BUCKET')}/media/image/")

        # delete old videos (we assume videos are not updated so we delete old to insert new ones)
        del_vid = delete_media(s3_client=s3_client, bucket=os.getenv("LANDING_ZONE_BUCKET"), 
                               prefix=f"{os.getenv('PERSISTENT_SUB_BUCKET')}/media/video/")

        if del_img and del_vid:
            try:
                objects = s3_client.list_objects_v2(Bucket=os.getenv("LANDING_ZONE_BUCKET"), Prefix=f"{os.getenv('TEMPORAL_SUB_BUCKET')}/")
                if "Contents" in objects:
                    moving_objects = [obj for obj in objects["Contents"] if not obj['Key'].endswith(("/", ".bak"))]
                    delete_objects = [obj for obj in objects["Contents"] if not obj['Key'].endswith("/")]
                    for obj in moving_objects:
                        logging.info(f"Preparing to move object {obj['Key']} to persistent storage.")
                        if obj['Key'].endswith(".json"):
                            ext = "json"
                            source = obj['Key'].split("/")[-1].split("_")[0]
                            sub_bucket = f"{source}"
                            convention_name = f"{os.getenv('PERSISTENT_SUB_BUCKET')}/{ext}/{sub_bucket}/"
                            convention_name += f"{source}#{datetime.now().strftime('%Y%m%d_%H%M%S')}#games.json"
                        elif obj['Key'].endswith(".jpg"):
                            game_id = obj['Key'].split("/")[-1].split("_")[0]
                            media_num = obj['Key'].split("/")[-1].split("_")[1].split(".")[0]
                            convention_name = f"{os.getenv('PERSISTENT_SUB_BUCKET')}/media/image/"
                            convention_name += f"{datetime.now().strftime('%Y%m%d_%H%M%S')}#{game_id}#{media_num}.jpg"
                        elif obj['Key'].endswith(".mp4"):
                            game_id = obj['Key'].split("/")[-1].split("_")[0]
                            media_num = obj['Key'].split("/")[-1].split("_")[1].split(".")[0]
                            convention_name = f"{os.getenv('PERSISTENT_SUB_BUCKET')}/media/video/"
                            convention_name += f"{datetime.now().strftime('%Y%m%d_%H%M%S')}#{game_id}#{media_num}.mp4"
                        s3_client.copy_object(
                            Bucket=os.getenv("LANDING_ZONE_BUCKET"),
                            CopySource={
                                "Bucket": os.getenv("LANDING_ZONE_BUCKET"),
                                "Key": obj["Key"]
                            },
                            Key=convention_name
                        )
                        logging.info(f"Copied object {obj['Key']} to persistent storage.")
                    s3_client.delete_objects(
                        Bucket=os.getenv("LANDING_ZONE_BUCKET"),
                        Delete={
                            'Objects': [{'Key': obj['Key']} for obj in delete_objects],
                            'Quiet': True
                        })
                    logging.info("Data successfully moved to persistent storage.")
                else:
                    logging.info("No objects found in temporal storage.")   
            except Exception:
                logging.exception("Error during moving data to persistent storage.")
                return

    except Exception:
        logging.exception("Error connecting to MinIO.")
        return
    

if __name__ == "__main__":
    main()