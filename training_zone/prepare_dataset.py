import json
import logging
import os
import random
from collections import defaultdict
from io import BytesIO

import boto3
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from global_scripts.utils import create_bucket, delete_items
from PIL import Image

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    force=True,
)


def prepare_dataset(s3_client):
    """
    Prepare dataset by:
    1. Loading descriptions from exploitation-zone
    2. Loading original images from exploitation-zone
    3. Selecting 3 images per game for train, 1 for val, 1 for test
    4. Copying selected images to training-zone
    5. Creating and saving CSV splits to MinIO
    """
    exploitation_bucket = os.getenv("EXPLOITATION_ZONE_BUCKET")
    training_bucket = os.getenv("TRAINING_ZONE_BUCKET")

    # Ensure training-zone bucket exists
    logging.info("Checking if training-zone bucket exists...")
    if not create_bucket(s3_client, training_bucket):
        logging.error("Failed to create or access training-zone bucket. Aborting.")
        return

    # Clear training-zone bucket
    logging.info("Clearing training-zone bucket...")
    delete_items(s3_client, bucket=training_bucket)

    # Step 1: Load descriptions from JSON in exploitation-zone
    logging.info("Loading descriptions from exploitation-zone...")
    objs = s3_client.list_objects_v2(Bucket=exploitation_bucket, Prefix="json/")
    game_descriptions = {}

    for obj in objs.get("Contents", []):
        if obj["Key"].endswith(".json"):
            data = s3_client.get_object(Bucket=exploitation_bucket, Key=obj["Key"])
            content = json.loads(data["Body"].read().decode("utf-8"))

            # Iterate over the games
            for game_id, data in content.items():
                desc = data["final_description"]
                if desc:
                    game_descriptions[game_id] = desc
                else:
                    logging.error(f"No description found for game ID {game_id}.")

    logging.info(f"Found descriptions for {len(game_descriptions)} games.")

    # Step 2: Copy JSON files to training-zone
    logging.info("Copying JSON files to training-zone...")
    for obj in objs.get("Contents", []):
        if obj["Key"].endswith(".json"):
            source_key = obj["Key"]
            filename = source_key.split("/")[-1]
            target_key = f"json/{filename}"

            s3_client.copy_object(
                Bucket=training_bucket,
                CopySource={"Bucket": exploitation_bucket, "Key": source_key},
                Key=target_key,
            )
    logging.info("JSON files copied to training-zone.")

    # Step 3: Load original images from exploitation-zone (only non-augmented)
    logging.info("Loading original images from exploitation-zone...")
    objs = s3_client.list_objects_v2(Bucket=exploitation_bucket, Prefix="media/image/")

    # Group images by game_id
    images_per_game = defaultdict(list)

    for obj in objs.get("Contents", []):
        key = obj["Key"]
        filename = key.split("/")[-1]

        # Extract game_id from filename (format: timestamp#game_id#number.jpg)
        parts = filename.split("#")
        if len(parts) >= 3:
            game_id = parts[1]
            if game_id in game_descriptions:
                images_per_game[game_id].append(key)

    logging.info(f"Found images for {len(images_per_game)} games.")

    # Step 4: For each game, randomly select images for train/val/test
    random.seed(42)  
    train_data = []
    val_data = []
    test_data = []

    logging.info("Selecting and copying images for each split...")

    for game_id, image_keys in images_per_game.items():
        if len(image_keys) != 5:
            logging.warning(f"Game {game_id} has {len(image_keys)} images (expected 5). Skipping.")
            continue

        # Shuffle and select
        shuffled_images = image_keys.copy()
        random.shuffle(shuffled_images)

        train_images = shuffled_images[:3]  # First 3 for training
        val_images = shuffled_images[3:4]  # 4th for validation
        test_images = shuffled_images[4:5]  # 5th for testing

        description = game_descriptions[game_id]

        # Process and copy train images
        for img_key in train_images:
            # Download, resize, and upload to training-zone
            target_key = copy_and_resize_image(s3_client, exploitation_bucket, training_bucket, img_key)
            train_data.append({"image_path": target_key, "description": description, "game_id": game_id})

        # Process and copy validation image
        for img_key in val_images:
            target_key = copy_and_resize_image(s3_client, exploitation_bucket, training_bucket, img_key)
            val_data.append({"image_path": target_key, "description": description, "game_id": game_id})

        # Process and copy test image
        for img_key in test_images:
            target_key = copy_and_resize_image(s3_client, exploitation_bucket, training_bucket, img_key)
            test_data.append({"image_path": target_key, "description": description, "game_id": game_id})

    # Create DataFrames
    train_df = pd.DataFrame(train_data)
    val_df = pd.DataFrame(val_data)
    test_df = pd.DataFrame(test_data)

    logging.info("Dataset splits created:")
    logging.info(f"  Train: {len(train_df)} images ({len(train_df['game_id'].unique())} games x 3 images)")
    logging.info(f"  Val:   {len(val_df)} images ({len(val_df['game_id'].unique())} games x 1 image)")
    logging.info(f"  Test:  {len(test_df)} images ({len(test_df['game_id'].unique())} games x 1 image)")

    # Save splits to MinIO
    logging.info("Saving splits to MinIO...")
    for split_name, split_df in zip(["train", "val", "test"], [train_df, val_df, test_df]):
        csv_obj = split_df.to_csv(index=False)
        try:
            s3_client.put_object(Bucket=training_bucket, Key=f"data_splits/{split_name}.csv", Body=csv_obj)
            logging.info(f"Saved {split_name}.csv with {len(split_df)} records.")
        except Exception:
            logging.exception(f"Error saving {split_name} split to MinIO.")

    logging.info("Dataset preparation completed successfully!")


def copy_and_resize_image(s3_client, source_bucket, target_bucket, source_key):
    """
    Copy an image from source bucket to target bucket, resizing it to 224x224.

    Returns:
        str: The target key in training-zone
    """
    # Download image
    resp = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    img_data = resp["Body"].read()

    # Resize to 224x224 (CLIP requirements)
    pil_img = Image.open(BytesIO(img_data)).convert("RGB")
    pil_img = pil_img.resize((224, 224), Image.Resampling.LANCZOS)

    # Save to buffer
    img_buffer = BytesIO()
    pil_img.save(img_buffer, format="JPEG")
    img_buffer.seek(0)

    # Upload to training-zone with simplified path
    filename = source_key.split("/")[-1]
    target_key = f"image/{filename}"

    s3_client.upload_fileobj(img_buffer, target_bucket, target_key)

    return target_key


if __name__ == "__main__":
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

    prepare_dataset(s3_client)
