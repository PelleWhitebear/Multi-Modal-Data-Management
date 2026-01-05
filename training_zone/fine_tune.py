import argparse
import logging
import os
import tempfile
from datetime import datetime

import torch
from dotenv import find_dotenv, load_dotenv
from fine_tune_utils import SteamDatasetHF, setup_config
from global_scripts.utils import minio_init
from peft import LoraConfig, get_peft_model
from torch.amp.autocast_mode import autocast
from torch.amp.grad_scaler import GradScaler
from torch.optim import AdamW
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import BitsAndBytesConfig, CLIPModel, CLIPProcessor


def save_model_to_minio(s3_client, model, processor, bucket, minio_path):
    """
    Save model and processor to MinIO storage.

    :param s3_client: The S3 client connection
    :param model: The model to save
    :param processor: The processor to save
    :param bucket: The MinIO bucket name
    :param minio_path: The path inside the bucket where to save the model
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        # Save model and processor to temporary directory
        model.save_pretrained(temp_dir)
        processor.save_pretrained(temp_dir)

        # Upload all files to MinIO
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, temp_dir)
                minio_key = f"{minio_path}/{relative_path}".replace("\\", "/")

                with open(local_file_path, "rb") as f:
                    s3_client.put_object(Bucket=bucket, Key=minio_key, Body=f.read())
                logging.info(f"Uploaded {relative_path} to {bucket}/{minio_key}")


# Load environment
load_dotenv(find_dotenv())

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    force=True,
)


def main(args):
    technique = args.technique.lower()
    logging.info(f"Selected fine-tuning technique: {technique}")
    s3_client = minio_init()
    CONFIG = setup_config(technique)
    CONFIG["technique"] = technique

    # Load train and validation data from MinIO
    bucket = os.getenv("TRAINING_ZONE_BUCKET", "training-zone")
    logging.info("Loading training data from MinIO...")

    train_csv_response = s3_client.get_object(Bucket=bucket, Key="data_splits/train.csv")
    train_csv_data = train_csv_response["Body"].read().decode("utf-8")

    val_csv_response = s3_client.get_object(Bucket=bucket, Key="data_splits/val.csv")
    val_csv_data = val_csv_response["Body"].read().decode("utf-8")

    # Generate unique run identifier for saving to MinIO
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = f"{timestamp}_{technique}"
    logging.info(f"Training run: {run_name}")

    if technique == "qlora":
        if CONFIG["device"] != "cuda":
            raise RuntimeError("QLoRA requires CUDA. Please run on a GPU-enabled machine.")
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=CONFIG["load_in_4bit"],
            bnb_4bit_compute_dtype=getattr(torch, CONFIG["bnb_4bit_compute_dtype"]),
            bnb_4bit_quant_type=CONFIG["bnb_4bit_quant_type"],
            bnb_4bit_use_double_quant=CONFIG["bnb_4bit_use_double_quant"],
        )
        # Load model with 4-bit quantization
        logging.info(f"Loading model {CONFIG['model_id']} with 4-bit quantization on {CONFIG['device']}...")
        model = CLIPModel.from_pretrained(
            CONFIG["model_id"],
            quantization_config=bnb_config,
            device_map="auto",  # Automatically place layers on available devices
        )
    else:
        # Load model and processor
        logging.info(f"Loading model {CONFIG['model_id']} on {CONFIG['device']}...")
        model = CLIPModel.from_pretrained(CONFIG["model_id"]).to(CONFIG["device"])

    processor = CLIPProcessor.from_pretrained(CONFIG["model_id"])

    if technique in ["lora", "qlora"]:
        peft_config = LoraConfig(
            r=CONFIG["lora_r"],
            lora_alpha=CONFIG["lora_alpha"],
            target_modules=CONFIG["lora_matrices"],
            lora_dropout=CONFIG["lora_dropout"],
            bias="none",
        )
        # Get LoRA model: freezes base model and enables gradients for adapters (AxB)
        model = get_peft_model(model, peft_config)
        model.print_trainable_parameters()

    # Prepare data
    logging.info("Preparing data...")
    train_dataset = SteamDatasetHF(s3_client, train_csv_data, processor)
    val_dataset = SteamDatasetHF(s3_client, val_csv_data, processor)

    train_loader = DataLoader(train_dataset, batch_size=CONFIG["batch_size"], shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=CONFIG["batch_size"], shuffle=False)

    # Optimizer
    optimizer = AdamW(model.parameters(), lr=CONFIG["learning_rate"], weight_decay=CONFIG["weight_decay"])

    if technique == "fp16":
        scaler = GradScaler()

    # Training loop
    best_val_loss = float("inf")
    patience_counter = 0
    logging.info("Starting training...")
    for epoch in range(CONFIG["epochs"]):
        model.train()
        total_train_loss = 0

        for batch in tqdm(train_loader, desc=f"Epoch {epoch + 1}/{CONFIG['epochs']}"):
            # Move batch to device
            token_ids = batch["token_ids"].to(CONFIG["device"])
            attention_mask = batch["attention_mask"].to(CONFIG["device"])
            pixel_values = batch["pixel_values"].to(CONFIG["device"])

            optimizer.zero_grad()

            # Forward pass
            if technique == "fp16":
                with autocast("cuda", dtype=torch.float16):
                    outputs = model(
                        input_ids=token_ids,
                        attention_mask=attention_mask,
                        pixel_values=pixel_values,
                        return_loss=True,
                    )
                scaler.scale(outputs.loss).backward()
                scaler.step(optimizer)
                scaler.update()
            else:
                outputs = model(
                    input_ids=token_ids,
                    attention_mask=attention_mask,
                    pixel_values=pixel_values,
                    return_loss=True,
                )

                # Backward pass
                outputs.loss.backward()
                optimizer.step()

            total_train_loss += outputs.loss.item()

        avg_train_loss = total_train_loss / len(train_loader)

        # Validation
        model.eval()
        total_val_loss = 0
        with torch.no_grad():
            for batch in val_loader:
                token_ids = batch["token_ids"].to(CONFIG["device"])
                attention_mask = batch["attention_mask"].to(CONFIG["device"])
                pixel_values = batch["pixel_values"].to(CONFIG["device"])

                if technique == "fp16":
                    with autocast("cuda", dtype=torch.float16):
                        outputs = model(
                            input_ids=token_ids,
                            attention_mask=attention_mask,
                            pixel_values=pixel_values,
                            return_loss=True,
                        )
                else:
                    outputs = model(
                        input_ids=token_ids,
                        attention_mask=attention_mask,
                        pixel_values=pixel_values,
                        return_loss=True,
                    )

                total_val_loss += outputs.loss.item()

        avg_val_loss = total_val_loss / len(val_loader)

        logging.info(
            f"[Epoch {epoch + 1}/{CONFIG['epochs']}] Train Loss: {avg_train_loss:.4f} | Val Loss: {avg_val_loss:.4f}"
        )

        # Save best model (early stopping)
        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            patience_counter = 0

            # Save to MinIO storage
            bucket = os.getenv("TRAINING_ZONE_BUCKET", "training-zone")
            minio_model_path = f"models/{technique}/{run_name}"
            logging.info(f"New best model found. Saving to MinIO: {bucket}/{minio_model_path}...")

            save_model_to_minio(s3_client, model, processor, bucket, minio_model_path)
        else:
            patience_counter += 1
            logging.info(f"No improvement. Patience {patience_counter}/{CONFIG['patience']}")

            if patience_counter >= CONFIG["patience"]:
                logging.info("Stopped - Early stopping.")
                break

    logging.info("Training completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fine-tune CLIP model on multi-modal dataset.")
    parser.add_argument(
        "--technique",
        type=str,
        choices=["baseline", "fp32", "fp16", "lora", "qlora"],
        default="fp32",
        help="Fine-tuning technique to use.",
    )
    args = parser.parse_args()
    main(args)
