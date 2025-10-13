import boto3
import logging
from utils import *
from consts import *

setup_logging("move_to_persistent.log")

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
            objects = s3_client.list_objects_v2(Bucket=LANDING_ZONE_BUCKET, Prefix=f"{TEMPORAL_SUB_BUCKET}/")
            if "Contents" in objects:
                moving_objects = [obj for obj in objects["Contents"] if not obj['Key'].endswith(("/", ".bak"))]
                delete_objects = [obj for obj in objects["Contents"] if not obj['Key'].endswith("/")]
                for obj in moving_objects:
                    logging.info(f"Preparing to move object {obj['Key']} to persistent storage.")
                    if obj['Key'].endswith(".json"):
                        ext = "json"
                        source = obj['Key'].split("/")[-1].split("_")[0]
                        sub_bucket = f"{source}"
                        convention_name = f"{PERSISTENT_SUB_BUCKET}/{ext}/{sub_bucket}/"
                        convention_name += f"{source}#{datetime.now().strftime('%Y%m%d_%H%M%S')}#games.json"
                    elif obj['Key'].endswith(".jpg"):
                        game_id = obj['Key'].split("/")[-1].split("_")[0]
                        media_num = obj['Key'].split("/")[-1].split("_")[1].split(".")[0]
                        convention_name = f"{PERSISTENT_SUB_BUCKET}/media/image/"
                        convention_name += f"{datetime.now().strftime('%Y%m%d_%H%M%S')}#{game_id}#{media_num}.jpg"
                    elif obj['Key'].endswith(".mp4"):
                        game_id = obj['Key'].split("/")[-1].split("_")[0]
                        media_num = obj['Key'].split("/")[-1].split("_")[1].split(".")[0]
                        convention_name = f"{PERSISTENT_SUB_BUCKET}/media/video/"
                        convention_name += f"{datetime.now().strftime('%Y%m%d_%H%M%S')}#{game_id}#{media_num}.mp4"
                    s3_client.copy_object(
                        Bucket=LANDING_ZONE_BUCKET,
                        CopySource={
                            "Bucket": LANDING_ZONE_BUCKET,
                            "Key": obj["Key"]
                        },
                        Key=convention_name
                    )
                    logging.info(f"Copied object {obj['Key']} to persistent storage.")
                s3_client.delete_objects(
                    Bucket=LANDING_ZONE_BUCKET,
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