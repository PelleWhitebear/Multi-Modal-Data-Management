import os
import logging
import boto3
import argparse
import numpy as np
import cv2
import time
import io
import json
from PIL import Image
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction
from chromadb import HttpClient

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(levelname)s] - %(message)s',
        force=True,  # override any existing config
        # filemode='w',
        # filename='similarity_search.log'
    )

def main(args):
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
    
    # Booting up ChromaDB
    try:
        chroma_client = HttpClient(
            host="chroma",
            port=8000
        )
        logging.info("Connected to ChromaDB.")
    
    except Exception:
        logging.exception("Error connecting to ChromaDB.")
        return

    # Load game data from MinIO
    try:
        objs = s3_client.list_objects_v2(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Prefix="json/")
        if "Contents" not in objs:
            logging.error("No JSON files found in exploitation-zone.")
            return
        filename = ""
        for obj in objs["Contents"]:
            if obj["Key"].endswith("enhanced_games.json"):
                filename = obj["Key"]
                break

        game_obj = s3_client.get_object(Bucket=os.getenv("EXPLOITATION_ZONE_BUCKET"), Key=filename)
        games = json.loads(game_obj["Body"].read().decode("utf-8"))
        logging.info(f"Game data fetched.")
    
    except Exception:
        logging.exception("Error fetching game JSON from MinIO.")
        return

    logging.info(f"Performing similarity search with input type: {args.input_type}, output type: {args.output_type}, top-k: {args.top_k}")
    try:
        if args.input_type in ["image", "video"]:
            if not os.path.isfile(args.input_value):
                logging.error(f"File {args.input_value} does not exist.")
                return
            logging.info(f"Processing file input: {args.input_value}")
            if args.input_type == "image":
                with open(args.input_value, "rb") as f:
                    in_data = [np.array(Image.open(io.BytesIO(f.read())))]
            else:
                in_data = []
                cap = cv2.VideoCapture(args.input_value)
                total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                if total_frames <= 0:
                    logging.warning(f"Video file {args.input_value} has no frames. Skipping.")
                    return

                # Sample evenly spaced frames, leaving out first and last frames
                frame_count = 0
                frame_indices = np.linspace(0, total_frames - 1, int(os.getenv("NUM_FRAMES")) + 2, dtype=int)
                for frame_idx in frame_indices[1:-1]:  # Skip first and last frames -> Black frames
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                    ret, frame = cap.read()
                    if ret:
                        # Convert from BGR to RGB
                        frame_count += 1
                        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        frame_pil = np.array(frame_rgb)
                        in_data.append(frame_pil)
                cap.release()
                logging.info(f"Extracted {frame_count} frames from video {args.input_value}.")
        else:
            logging.info(f"Processing text input.")
            in_data = [args.input_value]
    except Exception:
        logging.exception(f"Error reading file data.")

    try: 
        collections = chroma_client.list_collections()
    except Exception:
        logging.exception("Error listing collections.")
        return

    for out_type in args.output_type:
        if out_type not in ["text", "image", "video"]:
            logging.warning(f"Unsupported output type: {out_type}")
            continue

        logging.info(f"Retrieving top {args.top_k} similar items for output type: {out_type}")
    
        collection = None
        for col in collections:
            if out_type in col.name:
                collection = chroma_client.get_collection(col.name)
        if collection is None:
            logging.error(f"No collection found for output type: {out_type}")
            continue

        try:
            if args.input_type == "text":
                results = collection.query(
                    query_texts=in_data,
                    n_results=args.top_k
                )
            else:
                results = collection.query(
                    query_images=in_data,
                    n_results=args.top_k
                )
            for id, distance in zip(results["ids"], results["distances"]):
                logging.info(f"@@@{out_type}###{id}###{distance}@@@")
        except Exception:
            logging.exception(f"Error querying collection for output type: {out_type}")
            continue

        logging.info(f"Retrieved items for output type: {out_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Similarity Search Service")
    parser.add_argument("--input-type", 
                        type=str, 
                        choices=["text", "image", "video"], 
                        required=True, 
                        help="Type of the input for similarity search")
    parser.add_argument("--input-value", 
                        type=str, 
                        required=True, 
                        help="If text, the text query; if image/video, the file path")
    parser.add_argument("--output-type", nargs="+", choices=["text", "image", "video"],
                        type=str, 
                        required=True, 
                        help="Type of the output for similarity search")
    parser.add_argument("--top-k", 
                        type=int, 
                        default=10, 
                        help="Number of top similar items to retrieve")
    args = parser.parse_args()
    try:
        main(args)
    except Exception:
        logging.exception("Unhandled exception in similarity search.")
        while True:
            pass