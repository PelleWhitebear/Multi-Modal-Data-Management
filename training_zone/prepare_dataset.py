import boto3
import json
import os
import logging
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from sklearn.model_selection import train_test_split

load_dotenv(find_dotenv())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True,  
)

def prepare_dataset(s3_client):
    # Load descriptions from JSON
    logging.info("Loading descriptions from JSON...")
    objs = s3_client.list_objects_v2(Bucket=os.getenv("TRAINING_ZONE_BUCKET"), Prefix='json/')
    game_descriptions = {}

    for obj in objs.get('Contents', []):
        if obj['Key'].endswith('.json'):
            data = s3_client.get_object(Bucket=os.getenv("TRAINING_ZONE_BUCKET"), Key=obj['Key'])
            content = json.loads(data['Body'].read().decode('utf-8'))

            # Iterate over the games
            for game_id, data in content.items():
                desc = data['final_description']
                if desc:
                    game_descriptions[game_id] = desc
                else:
                    logging.error(f"Not description found for game ID {game_id}.")
    
    logging.info(f"Found descriptions for {len(game_descriptions)} games.")

    # Load images and pair them with descriptions
    logging.info("Loading images...")

    # AWS S3 API return max 1000 objects per request (we have 2k images)
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=os.getenv("TRAINING_ZONE_BUCKET"), 
        Prefix='image/'
    )
    
    dataset = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Extract game ID
                key = obj['Key']
                if 'aug' in key:
                    game_id = key.split('#')[2]
                else:
                    game_id = key.split('#')[1]

                # Pair image with its game ID
                if game_id in game_descriptions:
                    dataset.append({
                        "image_path": key, 
                        "description": game_descriptions[game_id],
                        "game_id": game_id
                    })
                else:
                    logging.error(f"Game ID of image {key} ({game_id}) not found in description game IDs.")
    
    logging.info(f"Total image-text pairs: {len(dataset)}")
    df = pd.DataFrame(dataset)

    # Split by game ID (70/15/15)
    unique_games = df['game_id'].unique()
    train_ids, test_ids = train_test_split(unique_games, test_size=0.3, random_state=42)
    val_ids, test_ids = train_test_split(test_ids, test_size=0.5, random_state=42)

    train_df = df[df['game_id'].isin(train_ids)]
    val_df = df[df['game_id'].isin(val_ids)]
    test_df = df[df['game_id'].isin(test_ids)]
    
    # save splits
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "data_splits")

    train_df.to_csv(os.path.join(output_dir, "train.csv"), index=False)
    val_df.to_csv(os.path.join(output_dir, "val.csv"), index=False)
    test_df.to_csv(os.path.join(output_dir, "test.csv"), index=False)

    logging.info(f"Splits saved to {output_dir}: train ({len(train_df)}), val ({len(val_df)}), test ({len(test_df)})")

if __name__ == '__main__':
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