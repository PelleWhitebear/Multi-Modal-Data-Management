import io
import logging
import os
from io import StringIO

import albumentations as A
import boto3
import numpy as np
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from PIL import Image

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    force=True,
)

# Sequential random transformations: a transformation to an image can be
# applied several times at once, depending on probability p.
transform = A.Compose(
    [
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
        A.Resize(height=224, width=224),
    ]
)


def augment_image(img_data, num_augmentations=3):
    """
    Applies augmentation to an image bytes data.
    Returns a list of augmented image bytes (io.BytesIO).

    Args:
        img_data: Image bytes data
        num_augmentations: Number of augmented versions to create (default: 3)

    Returns:
        List of augmented image bytes (io.BytesIO)
    """
    try:
        # Convert bytes to numpy array
        np_image = np.array(Image.open(io.BytesIO(img_data)).convert("RGB"))

        augmented_images = []
        for _ in range(num_augmentations):
            # Apply transformation
            augmented = transform(image=np_image)["image"]

            # Convert back to bytes
            img_pil = Image.fromarray(augmented)
            bytes_buffer = io.BytesIO()
            img_pil.save(bytes_buffer, format="JPEG")
            bytes_buffer.seek(0)
            augmented_images.append(bytes_buffer)

        return augmented_images
    except Exception:
        logging.exception("Error during image augmentation.")
        return []


def main():
    """
    Augment ONLY the training split images.
    This function:
    1. Loads train.csv from MinIO
    2. For each image in train.csv, creates 3 augmented versions
    3. Uploads augmented images to MinIO
    4. Updates train.csv to include augmented images (300 original + 900 augmented = 1200 total)
    """
    # MinIO client connection
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

    training_bucket = os.getenv("TRAINING_ZONE_BUCKET")

    # Step 1: Load train.csv from MinIO
    logging.info("Loading train.csv from MinIO...")
    try:
        response = s3_client.get_object(Bucket=training_bucket, Key="data_splits/train.csv")
        csv_content = response["Body"].read().decode("utf-8")
        train_df = pd.read_csv(StringIO(csv_content))
        logging.info(f"Loaded train.csv with {len(train_df)} original images.")
    except Exception:
        logging.exception("Error loading train.csv from MinIO.")
        return

    # Step 2: Augment each image in train split
    augmented_rows = []
    total_images = len(train_df)
    num_augmentations = 3  # Create 3 augmented versions per image

    logging.info(f"Starting augmentation for {total_images} training images...")
    logging.info(f"Each image will generate {num_augmentations} augmented versions.")

    for idx, row in train_df.iterrows():
        image_path = row["image_path"]
        description = row["description"]
        game_id = row["game_id"]

        if (idx + 1) % 50 == 0:
            logging.info(f"Processing image {idx + 1}/{total_images}...")

        try:
            # Download original image from training-zone
            resp = s3_client.get_object(Bucket=training_bucket, Key=image_path)
            img_data = resp["Body"].read()

            # Generate augmented images
            augmented_images = augment_image(img_data, num_augmentations=num_augmentations)

            if len(augmented_images) != num_augmentations:
                logging.warning(
                    f"Expected {num_augmentations} augmented images for {image_path}, got {len(augmented_images)}"
                )

            # Upload each augmented image and add to DataFrame
            original_filename = image_path.split("/")[-1]

            for aug_idx, aug_img_buffer in enumerate(augmented_images):
                # Create new filename: aug0#original_name, aug1#original_name, etc.
                aug_filename = f"aug{aug_idx}#{original_filename}"
                aug_key = f"image/{aug_filename}"

                # Upload to MinIO
                s3_client.upload_fileobj(aug_img_buffer, training_bucket, aug_key)

                # Add to augmented rows list
                augmented_rows.append({"image_path": aug_key, "description": description, "game_id": game_id})

        except Exception as e:
            logging.error(f"Error augmenting image {image_path}: {e}")
            continue

    logging.info(f"Successfully created {len(augmented_rows)} augmented images.")

    # Step 3: Combine original and augmented data
    augmented_df = pd.DataFrame(augmented_rows)
    updated_train_df = pd.concat([train_df, augmented_df], ignore_index=True)

    logging.info(
        f"Updated train dataset: {len(train_df)} original + {len(augmented_df)} augmented = {len(updated_train_df)} total"
    )

    # Step 4: Save updated train.csv to MinIO
    logging.info("Saving updated train.csv to MinIO...")
    try:
        csv_obj = updated_train_df.to_csv(index=False)
        s3_client.put_object(Bucket=training_bucket, Key="data_splits/train.csv", Body=csv_obj)
        logging.info(f"Successfully saved updated train.csv with {len(updated_train_df)} entries.")
    except Exception:
        logging.exception("Error saving updated train.csv to MinIO.")
        return

    logging.info("Data augmentation completed successfully!")
    logging.info(f"Training images: {len(updated_train_df)}")
    logging.info(f"  - Original: {len(train_df)}")
    logging.info(f"  - Augmented: {len(augmented_df)}")


if __name__ == "__main__":
    main()
