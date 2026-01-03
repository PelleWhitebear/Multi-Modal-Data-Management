import argparse
import logging
import os

import torch
from dotenv import find_dotenv, load_dotenv
from fine_tune_utils import SteamDatasetHF, setup_config, setup_experiment_dir
from global_scripts.utils import minio_init
from peft import LoraConfig, get_peft_model
from torch.amp.autocast_mode import autocast
from torch.amp.grad_scaler import GradScaler
from torch.optim import AdamW
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import BitsAndBytesConfig, CLIPModel, CLIPProcessor

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

    # Get train and validation data paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    train_csv_path = os.path.join(script_dir, "data_splits", "train.csv")
    val_csv_path = os.path.join(script_dir, "data_splits", "val.csv")

    # Create directory to save metadata of trained model
    run_dir = setup_experiment_dir(CONFIG, base_path=os.path.join(script_dir, "trained_models/v1"))

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
    train_dataset = SteamDatasetHF(s3_client, train_csv_path, processor, is_train=True)
    val_dataset = SteamDatasetHF(s3_client, val_csv_path, processor, is_train=False)

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
            logging.info(f"New best model found. Saving to {run_dir}/best_model...")

            best_model_path = os.path.join(run_dir, "best_model")
            model.save_pretrained(best_model_path)
            processor.save_pretrained(best_model_path)
        else:
            patience_counter += 1
            logging.info(f"No improvement. Patience {patience_counter}/{CONFIG['patience']}")

            if patience_counter >= CONFIG["patience"]:
                logging.info("Stopped - Early stopping.")
                break

    logging.info("Training completed.")


if __name__ == "__main__":
    # Do argparse here:
    parser = argparse.ArgumentParser(description="Fine-tune CLIP model on multi-modal dataset.")
    parser.add_argument(
        "--technique",
        type=str,
        choices=["fp32", "fp16", "lora", "qlora"],
        default="fp32",
        help="Fine-tuning technique to use.",
    )
    args = parser.parse_args()
    main(args)
