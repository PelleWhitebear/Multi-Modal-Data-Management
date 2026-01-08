"""
Analysis script to compare baseline and fp16 models.

This script:
1. Loads baseline and fp16 models from MinIO
2. Loads ALL games (100) with their 5 images + description
3. Calculates embeddings for both models
4. Analysis A: Finds top 3 games with LARGEST difference between baseline and fp16 embeddings
5. Analysis B: Finds top 3 games with LOWEST similarity in baseline
6. Saves results for visualization
"""

import json
import logging
import os
import tempfile
from collections import defaultdict
from io import BytesIO

import boto3
import numpy as np
import torch
from dotenv import find_dotenv, load_dotenv
from peft import PeftModel
from PIL import Image
from tqdm import tqdm
from transformers import CLIPModel, CLIPProcessor

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    force=True,
)


def load_model_from_minio(s3_client, technique, device):
    """
    Load model and processor from MinIO storage based on technique.

    :param s3_client: The S3 client connection
    :param technique: Fine-tuning technique ("baseline", "fp16", "fp32", etc.)
    :param device: The device to load the model on
    :return: Tuple of (model, processor)
    """
    bucket = os.getenv("TRAINING_ZONE_BUCKET", "training-zone")

    if technique.lower() == "baseline":
        logging.info("Loading baseline model (openai/clip-vit-base-patch32)...")
        model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
        processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
        return model, processor

    pattern = technique.lower()
    if pattern not in ["fp32", "fp16", "lora", "qlora"]:
        logging.error(f"Unknown technique '{technique}'. Defaulting to 'fp32'.")
        pattern = "fp32"

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

        # Load model and processor from temporary directory
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


def load_all_games_data(s3_client):
    """
    Load ALL games (100) with their images and descriptions from exploitation-zone.

    Returns:
        dict: {game_id: {"description": str, "images": [PIL.Image, ...], "image_keys": [str, ...]}}
    """
    exploitation_bucket = os.getenv("EXPLOITATION_ZONE_BUCKET")

    # Load descriptions
    logging.info("Loading descriptions from exploitation-zone...")
    game_descriptions = {}
    objs = s3_client.list_objects_v2(Bucket=exploitation_bucket, Prefix="json/")

    for obj in objs.get("Contents", []):
        if obj["Key"].endswith(".json"):
            data = s3_client.get_object(Bucket=exploitation_bucket, Key=obj["Key"])
            content = json.loads(data["Body"].read().decode("utf-8"))

            for game_id, data in content.items():
                desc = data.get("final_description")
                if desc:
                    game_descriptions[game_id] = desc

    logging.info(f"Found descriptions for {len(game_descriptions)} games.")

    # Load images grouped by game_id
    logging.info("Loading images from exploitation-zone...")
    games_data = defaultdict(lambda: {"description": "", "images": [], "image_keys": []})

    objs = s3_client.list_objects_v2(Bucket=exploitation_bucket, Prefix="media/image/")

    for obj in objs.get("Contents", []):
        key = obj["Key"]
        filename = key.split("/")[-1]

        # Extract game_id from filename (format: timestamp#game_id#number.jpg)
        parts = filename.split("#")
        if len(parts) >= 3:
            game_id = parts[1]

            if game_id in game_descriptions:
                # Download image
                try:
                    resp = s3_client.get_object(Bucket=exploitation_bucket, Key=key)
                    img_data = resp["Body"].read()
                    image = Image.open(BytesIO(img_data)).convert("RGB")

                    games_data[game_id]["images"].append(image)
                    games_data[game_id]["image_keys"].append(key)
                    games_data[game_id]["description"] = game_descriptions[game_id]
                except Exception as e:
                    logging.error(f"Error loading image {key}: {e}")

    # Filter games with exactly 5 images
    valid_games = {gid: data for gid, data in games_data.items() if len(data["images"]) == 5}

    logging.info(f"Loaded {len(valid_games)} games with 5 images each.")

    return valid_games


def compute_embeddings_for_all_games(model, processor, games_data, device):
    """
    Compute embeddings for all images and texts for all games.

    Returns:
        dict: {game_id: {"image_embeddings": tensor[5, dim], "text_embedding": tensor[1, dim]}}
    """
    embeddings = {}

    model.eval()
    with torch.no_grad():
        for game_id, data in tqdm(games_data.items(), desc="Computing embeddings"):
            images = data["images"]
            description = data["description"]

            # Compute image embeddings (5 images per game)
            image_embeds = []
            for img in images:
                img_inputs = processor(images=img, return_tensors="pt").to(device)
                img_features = model.get_image_features(**img_inputs)
                img_features = img_features / img_features.norm(dim=-1, keepdim=True)  # Normalize
                image_embeds.append(img_features.cpu())

            image_embeds = torch.cat(image_embeds, dim=0)  # [5, dim]

            # Compute text embedding
            text_inputs = processor(
                text=[description], return_tensors="pt", padding=True, truncation=True, max_length=77
            ).to(device)
            text_features = model.get_text_features(**text_inputs)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)  # Normalize

            embeddings[game_id] = {"image_embeddings": image_embeds, "text_embedding": text_features.cpu()}

    return embeddings


def analysis_a_largest_differences(baseline_embeddings, fp16_embeddings):
    """
    Find top 3 games with LARGEST difference between baseline and fp16 average embeddings.

    Returns:
        list: [(game_id, difference_score, baseline_avg, fp16_avg), ...]
    """
    logging.info("Analysis A: Finding games with largest embedding differences...")

    differences = []

    for game_id in baseline_embeddings.keys():
        # Baseline: average of 5 images + 1 text (6 embeddings total)
        baseline_imgs = baseline_embeddings[game_id]["image_embeddings"]  # [5, dim]
        baseline_txt = baseline_embeddings[game_id]["text_embedding"]  # [1, dim]
        baseline_all = torch.cat([baseline_imgs, baseline_txt], dim=0)  # [6, dim]
        baseline_avg = baseline_all.mean(dim=0)  # [dim]

        # FP16: average of 5 images + 1 text
        fp16_imgs = fp16_embeddings[game_id]["image_embeddings"]  # [5, dim]
        fp16_txt = fp16_embeddings[game_id]["text_embedding"]  # [1, dim]
        fp16_all = torch.cat([fp16_imgs, fp16_txt], dim=0)  # [6, dim]
        fp16_avg = fp16_all.mean(dim=0)  # [dim]

        # Euclidean distance between average embeddings
        diff = torch.norm(baseline_avg - fp16_avg).item()

        differences.append((game_id, diff, baseline_avg.numpy(), fp16_avg.numpy()))

    # Sort by largest difference
    differences.sort(key=lambda x: x[1], reverse=True)

    top_3 = differences[:3]

    logging.info("\nTop 3 games with LARGEST embedding differences (baseline vs fp16):")
    for i, (game_id, diff, _, _) in enumerate(top_3, 1):
        logging.info(f"  {i}. Game {game_id}: difference = {diff:.4f}")

    return top_3


def analysis_b_lowest_similarity(baseline_embeddings):
    """
    Find top 3 games with LOWEST average cosine similarity in baseline.
    (These are the games where baseline performed worst)

    Returns:
        list: [(game_id, avg_similarity), ...]
    """
    logging.info("\nAnalysis B: Finding games with lowest similarity in baseline...")

    similarities = []

    for game_id, embeds in baseline_embeddings.items():
        image_embeds = embeds["image_embeddings"]  # [5, dim]
        text_embed = embeds["text_embedding"]  # [1, dim]

        # Compute cosine similarity between each image and the text
        sims = []
        for i in range(5):
            img_embed = image_embeds[i : i + 1]  # [1, dim]
            sim = torch.nn.functional.cosine_similarity(img_embed, text_embed, dim=1).item()
            sims.append(sim)

        avg_sim = np.mean(sims)
        similarities.append((game_id, avg_sim))

    # Sort by lowest similarity
    similarities.sort(key=lambda x: x[1])

    top_3_worst = similarities[:3]

    logging.info("\nTop 3 games with LOWEST similarity in baseline:")
    for i, (game_id, sim) in enumerate(top_3_worst, 1):
        logging.info(f"  {i}. Game {game_id}: avg similarity = {sim:.4f}")

    return top_3_worst


def save_results(baseline_embeddings, fp16_embeddings, games_data, analysis_a_results, analysis_b_results):
    """
    Save embeddings and analysis results for visualization.
    """
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "analysis_results")
    os.makedirs(output_dir, exist_ok=True)

    logging.info(f"\nSaving results to {output_dir}...")

    # Save embeddings as numpy arrays
    baseline_embeds_dict = {}
    fp16_embeds_dict = {}

    for game_id in baseline_embeddings.keys():
        baseline_embeds_dict[game_id] = {
            "image_embeddings": baseline_embeddings[game_id]["image_embeddings"].numpy(),
            "text_embedding": baseline_embeddings[game_id]["text_embedding"].numpy(),
        }
        fp16_embeds_dict[game_id] = {
            "image_embeddings": fp16_embeddings[game_id]["image_embeddings"].numpy(),
            "text_embedding": fp16_embeddings[game_id]["text_embedding"].numpy(),
        }

    np.save(os.path.join(output_dir, "embeddings_baseline.npy"), baseline_embeds_dict, allow_pickle=True)
    np.save(os.path.join(output_dir, "embeddings_fp16.npy"), fp16_embeds_dict, allow_pickle=True)

    # Save game metadata
    metadata = {
        "analysis_a_top3_largest_differences": [
            {
                "game_id": game_id,
                "difference": float(diff),
                "description": games_data[game_id]["description"][:200] + "...",
            }
            for game_id, diff, _, _ in analysis_a_results
        ],
        "analysis_b_top3_lowest_similarity": [
            {
                "game_id": game_id,
                "avg_similarity": float(sim),
                "description": games_data[game_id]["description"][:200] + "...",
            }
            for game_id, sim in analysis_b_results
        ],
    }

    with open(os.path.join(output_dir, "game_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=4)

    logging.info("Results saved successfully!")
    logging.info("  - embeddings_baseline.npy")
    logging.info("  - embeddings_fp16.npy")
    logging.info("  - game_metadata.json")


def main():
    # Initialize MinIO client
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

    device = "cuda" if torch.cuda.is_available() else "cpu"
    logging.info(f"Using device: {device}")

    # Step 1: Load models
    logging.info("\n" + "=" * 60)
    logging.info("STEP 1: Loading models from MinIO")
    logging.info("=" * 60)

    baseline_model, baseline_processor = load_model_from_minio(s3_client, "baseline", device)
    fp16_model, fp16_processor = load_model_from_minio(s3_client, "lora", device)

    # Step 2: Load all games data
    logging.info("\n" + "=" * 60)
    logging.info("STEP 2: Loading all games data")
    logging.info("=" * 60)

    games_data = load_all_games_data(s3_client)

    # Step 3: Compute embeddings for baseline
    logging.info("\n" + "=" * 60)
    logging.info("STEP 3: Computing embeddings for BASELINE model")
    logging.info("=" * 60)

    baseline_embeddings = compute_embeddings_for_all_games(
        baseline_model, baseline_processor, games_data, device
    )

    # Step 4: Compute embeddings for fp16
    logging.info("\n" + "=" * 60)
    logging.info("STEP 4: Computing embeddings for FP16 model")
    logging.info("=" * 60)

    fp16_embeddings = compute_embeddings_for_all_games(fp16_model, fp16_processor, games_data, device)

    # Step 5: Analysis A - Largest differences
    logging.info("\n" + "=" * 60)
    logging.info("STEP 5: Analysis A - Games with largest embedding differences")
    logging.info("=" * 60)

    analysis_a_results = analysis_a_largest_differences(baseline_embeddings, fp16_embeddings)

    # Step 6: Analysis B - Lowest similarity in baseline
    logging.info("\n" + "=" * 60)
    logging.info("STEP 6: Analysis B - Games with lowest similarity in baseline")
    logging.info("=" * 60)

    analysis_b_results = analysis_b_lowest_similarity(baseline_embeddings)

    # Step 7: Save results
    logging.info("\n" + "=" * 60)
    logging.info("STEP 7: Saving results for visualization")
    logging.info("=" * 60)

    save_results(baseline_embeddings, fp16_embeddings, games_data, analysis_a_results, analysis_b_results)

    logging.info("\n" + "=" * 60)
    logging.info("ANALYSIS COMPLETE!")
    logging.info("=" * 60)
    logging.info("Next steps:")
    logging.info("  1. Run visualizations.py to create PCA plots and heatmaps")
    logging.info("  2. Check analysis_results/ for saved data")


if __name__ == "__main__":
    main()
