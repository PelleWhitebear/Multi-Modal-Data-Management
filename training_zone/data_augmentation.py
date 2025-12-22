import os
import logging
import boto3
import io
import numpy as np
import albumentations as A
from PIL import Image
from dotenv import load_dotenv, find_dotenv
from global_scripts.utils import delete_items
from botocore.exceptions import ClientError

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True,  
)

# Sequential random transformations: a transformation to an image can be 
# applied several times at once, depending on probability p.
transform = A.Compose([
    # Select a random rectangular region of the image (area defined in scale -> 80-100%)
    A.RandomResizedCrop(height=224, width=224, scale=(0.8, 1.0), p=0.8),

    # Flip the image horizontally
    A.HorizontalFlip(p=0.5),

    # Modify color and quality of the pixels
    A.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1, p=0.5),

    # Add random Gaussian noise tho the image
    A.GaussNoise(var_limit=(10.0, 50.0), p=0.3),

    # Place 1-8 rectangular holes in the image (regularization)
    A.CoarseDropout(max_holes=8, max_height=32, max_width=32, min_holes=1, fill_value=0, p=0.3),

    # Rotate image randomly up to +-15 degrees
    A.Rotate(limit=15, p=0.3), 

    # Resize image to 224x224 pixels
    A.Resize(height=224, width=224)
])

def augment_image(img_data):
    """
    Applies augmentation to an image bytes data.
    Returns a list of augmented image bytes (io.BytesIO).
    """
    try:
        # Convert bytes to numpy array
        np_image = np.array(Image.open(io.BytesIO(img_data)).convert("RGB"))

        augmented_images = []
        n_aug = int(os.getenv("NUM_AUGMENTED_IMAGES"))
        for _ in range(n_aug):
            # Apply transformation
            augmented = transform(image=np_image)["image"]

            # Convert back to bytes
            img_pil = Image.fromarray(augmented)
            bytes = io.BytesIO()
            img_pil.save(bytes, format="JPEG")
            bytes.seek(0)
            augmented_images.append(bytes)

        return augmented_images

    except Exception as e:
        logging.error(f"Error augmenting image: {e}")
        return []

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
    except Exception:
        logging.exception("Error connecting to MinIO.")
        return
    
    # Create target bucket
    target_name = os.getenv("TRAINING_ZONE_BUCKET")
    try:
        s3_client.head_bucket(Bucket=target_name)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404":
            try:
                s3_client.create_bucket(Bucket=target_name)
                logging.info(f"Created bucket '{target_name}'.")
            except ClientError as create_err:
                c_code = create_err.response.get("Error", {}).get("Code")
                if c_code == "BucketAlreadyOwnedByYou":
                    logging.info(f"Bucket '{target_name}' already exists.")
                else:
                    logging.error(f"Failed to create bucket '{target_name}': {create_err}")
        else:
            logging.error(f"Error checking bucket '{target_name}': {e}")

    # Empty target bucket training-zone before data augmentation
    delete_items(s3_client, bucket=target_name)

    exploitation_bucket = os.getenv("EXPLOITATION_ZONE_BUCKET")

    # Copy JSON data into the target bucket training-zone
    try:
        logging.info("Copying JSON data...")
        json_objs = s3_client.list_objects_v2(Bucket=exploitation_bucket, Prefix="json/")
        if "Contents" in json_objs:
            for obj in json_objs["Contents"]:
                if obj["Key"].endswith(".json"):
                    source_key = obj["Key"]
                    filename = source_key.split('/')[-1]
                    target_key = f"json/{filename}"
                    
                    s3_client.copy_object(
                        Bucket=target_name,
                        CopySource={'Bucket': exploitation_bucket, 'Key': source_key},
                        Key=target_key
                    )
                    logging.info(f"Copied {filename} to {target_key}")
    except Exception as e:
        logging.error(f"Error copying JSON files: {e}")
    
    # Images augmentation
    try:
        # Read images
        objs = s3_client.list_objects_v2(Bucket=exploitation_bucket, Prefix="media/image/")
        if "Contents" not in objs:
            logging.warning("No images found in exploitation zone.")
            return

        total_images = len(objs["Contents"])
        logging.info(f"Found {total_images}. Starting augmentation...")

        # Augment images
        for i, obj in enumerate(objs["Contents"]):
            # Extract game ID
            original_key = obj["Key"]
            img_name = original_key.split('/')[-1]
            game_id = img_name.split('#')[1]
            logging.info(f"Processing image {i}/{total_images} (Game ID: {game_id})")

            # Download image
            resp = s3_client.get_object(Bucket=exploitation_bucket, Key=original_key)
            img_data = resp['Body'].read()

            # Upload original image to target bucket training-zone
            try:
                # Before uploading it, we resize it to fit CLIP requirements
                pil_img = Image.open(io.BytesIO(img_data)).convert("RGB")
                pil_img = pil_img.resize((224, 224), Image.Resampling.LANCZOS)
                original_buf = io.BytesIO()
                pil_img.save(original_buf, format="JPEG")
                original_buf.seek(0)

                original_target_key = f"image/{img_name}"
                s3_client.upload_fileobj(original_buf, target_name, original_target_key)
            except Exception as e:
                logging.error(f"Failed to upload original {img_name}: {e}")
            
            # Generate and upload augmented images to the target bucket training-zone
            augmented_images = augment_image(img_data)
            for id_aug, aug_img in enumerate(augmented_images):
                new_key = f"image/aug{id_aug}#{img_name}"
                try:
                    s3_client.upload_fileobj(aug_img, target_name, new_key)
                except Exception as e:
                    logging.error(f"Failed to upload {new_key}: {e}")

        logging.info("Data augmentation completed successfully.")
    
    except Exception:
        logging.exception("Error during image augmentation.")

if __name__ == '__main__':
    main()