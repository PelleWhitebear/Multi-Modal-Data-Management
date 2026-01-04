# 1. Load Data (All images and texts)
# 2. Load models (FP32, FP16, LoRA, QLoRA)
# 3. Calculate Embeddings for all images and texts
# 4. Calculate Similarity Matrix
# 5. Calculate Metrics (recall@K, mAP@K, MRR, mean loss, mean similarity)
# 6. Store Results
# 7. Generate Summary Report

import argparse
import csv
import json
import logging
import os
import tempfile
from io import BytesIO, StringIO

import torch
from dotenv import find_dotenv, load_dotenv
from global_scripts.utils import minio_init
from metrics import compute_all_metrics
from peft import PeftModel
from PIL import Image
from tqdm import tqdm
from transformers import CLIPModel, CLIPProcessor

# Load environment
load_dotenv(find_dotenv())

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    force=True,
)


def load_model_from_minio(s3_client, technique, device):
    """
    Load model and processor from MinIO storage based on technique.

    :param s3_client: The S3 client connection
    :param technique: Fine-tuning technique ("fp32", "fp16", "lora", "qlora")
    :param device: The device to load the model on
    :return: Tuple of (model, processor)
    """
    bucket = os.getenv("TRAINING_ZONE_BUCKET", "training-zone")

    pattern = technique.lower()
    if pattern not in ["fp32", "fp16", "lora", "qlora"]:
        logging.error(f"Unknown technique '{technique}'. Defaulting to 'fp32'.")
        pattern = "fp32"

    if pattern == "qlora" and device != "cuda":
        raise EnvironmentError("QLoRA requires a CUDA-capable GPU, but none was found.")

    # List all models with this technique and get the most recent one
    technique_prefix = f"models/{pattern}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=technique_prefix, Delimiter="/")
    matching_dirs = []

    for prefix in response.get("CommonPrefixes", []):
        dir_name = prefix["Prefix"].rstrip("/")
        matching_dirs.append(dir_name)

    if not matching_dirs:
        raise FileNotFoundError(
            f"No trained model found for technique '{technique}' in bucket '{bucket}/{technique_prefix}'"
        )

    # Sort by timestamp (assuming format: YYYYMMDD_HHMMSS_...)
    matching_dirs.sort(reverse=True)
    latest_model_dir = matching_dirs[0]
    minio_path = f"{latest_model_dir}"

    logging.info(f"Loading model from {bucket}/{minio_path}...")

    with tempfile.TemporaryDirectory() as temp_dir:
        # List all objects in the minio_path
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=minio_path)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            # Get relative path from minio_path
            relative_path = key[len(minio_path) :].lstrip("/")
            if not relative_path:
                continue

            local_file_path = os.path.join(temp_dir, relative_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Download file
            file_response = s3_client.get_object(Bucket=bucket, Key=key)
            with open(local_file_path, "wb") as f:
                f.write(file_response["Body"].read())
            logging.info(f"Downloaded {relative_path} from {bucket}/{key}")

        # Load model and processor from temporary directory
        # Check if it's a PEFT model by looking for adapter_config.json
        adapter_config_path = os.path.join(temp_dir, "adapter_config.json")

        if os.path.exists(adapter_config_path):
            # It's a PEFT model - load base model first, then apply PEFT
            logging.info("Detected PEFT model (LoRA/QLoRA), loading with PEFT...")
            base_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
            model = PeftModel.from_pretrained(base_model, temp_dir).to(device)
        else:
            # Regular fine-tuned model
            model = CLIPModel.from_pretrained(temp_dir).to(device)

        processor = CLIPProcessor.from_pretrained(temp_dir)

        return model, processor


def load_test_data(s3_client) -> list:
    """
    Load test data from minio S3 storage.

    Returns:
        list: List of test data items.
    """
    logging.info("Loading test data from S3...")
    test_data = []
    try:
        response = s3_client.get_object(Bucket=os.getenv("TRAINING_ZONE_BUCKET"), Key="data_splits/test.csv")
        csv_content = response["Body"].read().decode("utf-8")

        # Parse CSV properly to handle quoted fields with commas
        csv_reader = csv.DictReader(StringIO(csv_content))
        test_data = [
            {"image_path": row["image_path"], "description": row["description"], "id": row["game_id"]}
            for row in csv_reader
        ]
    except Exception as e:
        logging.error(f"Error loading test data: {e}")
    return test_data


def load_images_and_descriptions(s3_client, test_data) -> list:
    """
    Load images and descriptions from S3 for the test data.

    Args:
        s3_client: The S3 client connection.
        test_data (list): List of test data items with image paths and descriptions.

    Returns:
        list: List of dictionaries with 'image' (PIL Image) and 'description' (str).
    """
    bucket = os.getenv("TRAINING_ZONE_BUCKET", "training-zone")
    loaded_data = []

    logging.info(f"Loading {len(test_data)} images from S3...")

    for idx, item in enumerate(test_data):
        try:
            image_path = item["image_path"]
            description = item["description"]
            id = item["id"]

            # Download image from MinIO
            response = s3_client.get_object(Bucket=bucket, Key=image_path)
            image_bytes = response["Body"].read()

            # Load image with PIL
            image = Image.open(BytesIO(image_bytes)).convert("RGB")

            loaded_data.append({"image": image, "description": description, "id": id})

            if (idx + 1) % 50 == 0:
                logging.info(f"Loaded {idx + 1}/{len(test_data)} images...")

        except Exception as e:
            logging.error(f"Error loading image {item.get('image_path', 'unknown')}: {e}")
            continue

    logging.info(f"Successfully loaded {len(loaded_data)}/{len(test_data)} images.")
    return loaded_data


def main(args):
    s3_client = minio_init()
    test_data = load_test_data(s3_client)
    test_data = load_images_and_descriptions(s3_client, test_data)

    # Load model based on technique
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logging.info(f"Using device: {device}")
    logging.info(f"Selected technique: {args.technique}")

    if args.technique.lower() == "baseline":
        model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
        processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
    else:
        try:
            model, processor = load_model_from_minio(s3_client, args.technique, device)
        except Exception as e:
            logging.error(f"Failed to load model: {e}")
            return

    model.eval()

    # Step 1: Compute embeddings for all images and texts
    logging.info("Computing embeddings for all images and texts...")
    image_embeddings = []
    text_embeddings = []
    game_ids = []

    with torch.no_grad():
        for item in tqdm(test_data, desc="Computing embeddings"):
            image = item["image"]
            description = item["description"]
            game_id = item["id"]
            game_ids.append(game_id)

            # Process image
            image_inputs = processor(images=image, return_tensors="pt").to(device)
            image_features = model.get_image_features(**image_inputs)
            image_embeddings.append(image_features.cpu())

            # Process text
            text_inputs = processor(
                text=[description], return_tensors="pt", padding=True, truncation=True, max_length=77
            ).to(device)
            text_features = model.get_text_features(**text_inputs)
            text_embeddings.append(text_features.cpu())

    # Stack embeddings into tensors
    image_embeddings = torch.cat(image_embeddings, dim=0)  # [N, embed_dim]
    text_embeddings = torch.cat(text_embeddings, dim=0)  # [N, embed_dim]

    # Normalize embeddings
    image_embeddings = image_embeddings / image_embeddings.norm(dim=-1, keepdim=True)
    text_embeddings = text_embeddings / text_embeddings.norm(dim=-1, keepdim=True)

    logging.info(f"Embeddings shape - Images: {image_embeddings.shape}, Texts: {text_embeddings.shape}")

    # Step 2: Compute similarity matrix (text-to-image retrieval)
    # For each text query, compute similarity to all images
    similarity_matrix = torch.matmul(text_embeddings, image_embeddings.T)  # [N, N]

    # Step 3: Calculate metrics
    logging.info("Calculating metrics...")

    # Mean cosine similarity (diagonal elements = correct pairs)
    mean_cosine_sim = torch.diagonal(similarity_matrix).mean().item()

    # Mean loss (contrastive loss approximation)
    # Loss = -log(exp(sim_correct) / sum(exp(sim_all)))
    logits = similarity_matrix * 100  # Temperature scaling (typical value)
    labels = torch.arange(len(test_data))
    loss = torch.nn.functional.cross_entropy(logits, labels)
    mean_loss = loss.item()

    # Correct indices (diagonal: text[i] matches image[i])
    correct_indices = torch.arange(len(test_data))

    # Compute retrieval metrics
    metrics = compute_all_metrics(similarity_matrix, correct_indices, k_values=[1, 5, 10])

    # Add additional metrics
    metrics["mean_cosine_similarity"] = mean_cosine_sim
    metrics["mean_loss"] = mean_loss

    # Convert recall and map values to percentages for saving
    metrics_to_save = {}
    for key, value in metrics.items():
        if key.startswith("recall@") or key.startswith("map@") or key == "mrr":
            metrics_to_save[key] = value * 100  # Convert to percentage
        else:
            metrics_to_save[key] = value

    # Step 4: Log and save results
    logging.info(f"Results for {args.technique.upper()}:")
    logging.info("=" * 50)
    logging.info(f"Mean Cosine Similarity: {mean_cosine_sim:.4f}")
    logging.info(f"Mean Loss: {mean_loss:.4f}")
    logging.info(f"Recall@1: {metrics['recall@1'] * 100:.2f}%")
    logging.info(f"Recall@5: {metrics['recall@5'] * 100:.2f}%")
    logging.info(f"Recall@10: {metrics['recall@10'] * 100:.2f}%")
    logging.info(f"mAP@1: {metrics['map@1'] * 100:.2f}%")
    logging.info(f"mAP@5: {metrics['map@5'] * 100:.2f}%")
    logging.info(f"mAP@10: {metrics['map@10'] * 100:.2f}%")
    logging.info(f"MRR: {metrics['mrr'] * 100:.2f}%")
    logging.info("=" * 50)

    # Save results to file
    results_dir = os.path.join(os.path.dirname(__file__), "experiment_results")
    os.makedirs(results_dir, exist_ok=True)

    results_file = os.path.join(results_dir, f"{args.technique}_results.json")
    with open(results_file, "w") as f:
        json.dump(metrics_to_save, f, indent=4)

    logging.info(f"Results saved to {results_file}")

    return metrics_to_save


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run multi-modal data management experiments.")
    parser.add_argument(
        "--technique",
        type=str,
        required=True,
        choices=["baseline", "fp32", "fp16", "lora", "qlora"],
        help="Model fine-tuning technique to use.",
    )
    args = parser.parse_args()

    logging.info(f"Starting experiment: {args.technique}")

    main(args)
