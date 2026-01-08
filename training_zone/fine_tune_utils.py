import logging
import os
from io import BytesIO, StringIO

import pandas as pd
import torch
from PIL import Image
from torch.utils.data import Dataset
from torchvision import transforms  

BASE_CONFIG = {
    "model_id": "openai/clip-vit-base-patch32",
    "epochs":100,
    "batch_size": 8,
    "learning_rate": 5e-6,
    #"learning_rate": 3e-5,
    "patience": 5,
    "weight_decay": 0.1,
    "device": "cuda" if torch.cuda.is_available() else "cpu",
}

LORA_CONFIG = {
    "lora_r": 16,  # Rank of the LoRA matrices
    "lora_alpha": 16,  # Alphascaling factor
    "lora_matrices": [
        "q_proj",
        "v_proj",
    ],  
    "lora_dropout": 0.3,
    "device": "cuda" if torch.cuda.is_available() else "cpu",
}

QUANT_CONFIG = {
    # Quantization config (4-bit)
    "load_in_4bit": True,
    "bnb_4bit_compute_dtype": "float32",  # Computation dtype
    "bnb_4bit_quant_type": "nf4",  # Quantization type: "nf4" or "fp4"
    "bnb_4bit_use_double_quant": True,  # Nested quantization for more memory savings
    "device": "cuda" if torch.cuda.is_available() else "cpu",
}


def setup_config(technique):
    match technique:
        case "qlora":
            CONFIG = {**BASE_CONFIG, **LORA_CONFIG, **QUANT_CONFIG}
        case "lora":
            CONFIG = {**BASE_CONFIG, **LORA_CONFIG}
        case _:
            CONFIG = BASE_CONFIG
    return CONFIG


class SteamDatasetHF(Dataset):
    def __init__(self, s3_client, csv_data, processor):
        """
        Dataset for loading pre-processed images and text from MinIO.

        Note: Data augmentation is now done offline during dataset preparation,
        so we don't apply any transforms here. The train.csv already contains
        both original and augmented images.

        :param s3_client: MinIO S3 client
        :param csv_data: Either a pandas DataFrame or a CSV string/bytes
        :param processor: CLIP processor
        """
        self.s3_client = s3_client

        # Accept DataFrame or CSV
        if isinstance(csv_data, pd.DataFrame):
            self.data = csv_data
        elif isinstance(csv_data, (str, bytes)):
            self.data = pd.read_csv(StringIO(csv_data) if isinstance(csv_data, str) else BytesIO(csv_data))
        else:
            raise ValueError("csv_data must be a pandas DataFrame, string, or bytes")

        self.processor = processor

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        row = self.data.iloc[idx]
        image_key = row["image_path"]
        desc = row["description"]

        # Fetch image (already pre-processed and augmented if needed)
        try:
            resp = self.s3_client.get_object(Bucket=os.getenv("TRAINING_ZONE_BUCKET"), Key=image_key)
            img_data = resp["Body"].read()
            image = Image.open(BytesIO(img_data)).convert("RGB")
        except Exception as e:
            logging.error(f"Error loading {image_key}: {e}")
            image = Image.new("RGB", (224, 224), color="black")

        # Processor: tokenizer for text + processor for images
        # - Tokenize text and return token IDs
        # - Process images and return pixel values (3,224,224)
        inputs = self.processor(
            text=[desc], images=image, return_tensors="pt", padding="max_length", truncation=True
        )

        return {
            "token_ids": inputs["input_ids"].squeeze(0),
            "attention_mask": inputs["attention_mask"].squeeze(0),
            "pixel_values": inputs["pixel_values"].squeeze(0),
        }


# ============================================================================
# OPTIONAL: Dataset with On-The-Fly Augmentation
# ============================================================================
# This class is kept for backward compatibility or if wanted to revert
# to on-the-fly augmentation instead of pre-computed augmented images.
# To use it, replace SteamDatasetHF with SteamDatasetHF_WithAugmentation
# in fine_tune.py and pass is_train=True for training data.
# ============================================================================


class SteamDatasetHF_WithAugmentation(Dataset):
    """
    Alternative dataset class that applies data augmentation on-the-fly.
    This is NOT used in the current pipeline (we use pre-computed augmented images).
    Kept here for reference or if you want to revert to on-the-fly augmentation.
    """

    def __init__(self, s3_client, csv_data, processor, is_train=False):
        """
        :param s3_client: MinIO S3 client
        :param csv_data: Either a pandas DataFrame or a CSV string/bytes
        :param processor: CLIP processor
        :param is_train: Whether to apply training augmentations on-the-fly
        """
        self.s3_client = s3_client

        # Accept DataFrame or CSV content
        if isinstance(csv_data, pd.DataFrame):
            self.data = csv_data
        elif isinstance(csv_data, (str, bytes)):
            self.data = pd.read_csv(StringIO(csv_data) if isinstance(csv_data, str) else BytesIO(csv_data))
        else:
            raise ValueError("csv_data must be a pandas DataFrame, string, or bytes")

        self.processor = processor
        self.is_train = is_train

        # Define Dynamic Transforms (Run on CPU before CLIP Processor)
        # Only applied when is_train=True
        self.train_transform = transforms.Compose(
            [
                # Forces model to learn parts of the image, not just the whole
                transforms.RandomResizedCrop(size=224, scale=(0.8, 1.0)),
                # Randomly flip horizontally (Great for most games)
                transforms.RandomHorizontalFlip(p=0.5),
                # Randomly change brightness/contrast so model doesn't rely on exact colors
                transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2, hue=0.1),
                # (Optional) Random rotation if appropriate for your game UI/style
                transforms.RandomRotation(degrees=15),
            ]
        )

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        row = self.data.iloc[idx]
        image_key = row["image_path"]
        desc = row["description"]

        # Fetch image
        try:
            resp = self.s3_client.get_object(Bucket=os.getenv("TRAINING_ZONE_BUCKET"), Key=image_key)
            img_data = resp["Body"].read()
            image = Image.open(BytesIO(img_data)).convert("RGB")

            # Apply augmentation on-the-fly if training
            if self.is_train:
                image = self.train_transform(image)
        except Exception as e:
            logging.error(f"Error loading {image_key}: {e}")
            image = Image.new("RGB", (224, 224), color="black")

        # Processor: tokenizer for text + processor for images
        inputs = self.processor(
            text=[desc], images=image, return_tensors="pt", padding="max_length", truncation=True
        )

        return {
            "token_ids": inputs["input_ids"].squeeze(0),
            "attention_mask": inputs["attention_mask"].squeeze(0),
            "pixel_values": inputs["pixel_values"].squeeze(0),
        }
