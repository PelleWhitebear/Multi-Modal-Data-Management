import os
import logging
import pandas as pd
import torch
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from transformers import CLIPModel, CLIPProcessor
from torch.utils.data import Dataset, DataLoader
from PIL import Image
from io import BytesIO
from torch.optim import AdamW

# Load environment
load_dotenv(find_dotenv())

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    force=True,  
)

# MinIO setup
s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

# Hyperparameters
CONFIG = {
    "run_name": "fully_finetuned_fp32",
    "model_id": "openai/clip-vit-base-patch32", 
    "epochs": 10,
    "batch_size": 32,
    "learning_rate": 5e-6,
    "patience": 3, 
    "weight_decay": 0.1,
    "warmup_steps": 100,
    "device": "cuda" if torch.cuda.is_available() else "cpu"
}

def setup_experiment_dir(base_path="trained_models/v1"):
    """
    Creates a unique folder for this training run and saves the config.
    Structure: trained_models/v1/YYYYMMDD_HHMMSS_run_name/
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir_name = f"{timestamp}_{CONFIG['run_name']}"
    run_dir = os.path.join(base_path, run_dir_name)
    
    os.makedirs(run_dir, exist_ok=True)
    
    # Save information about hyperparameters
    with open(os.path.join(run_dir, "hyperparameters.json"), "w") as f:
        json.dump(CONFIG, f, indent=4)
        
    return run_dir

class SteamDatasetHF(Dataset):
    def __init__(self, csv_file, processor):
        self.data = pd.read_csv(csv_file)
        self.processor = processor
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        row = self.data.iloc[idx]
        image_key = row['image_path']
        desc = row['description']

        # Fetch image
        try:
            resp = s3_client.get_object(
                Bucket=os.getenv('TRAINING_ZONE_BUCKET'),
                Key=image_key
            )
            img_data = resp['Body'].read()
            image = Image.open(BytesIO(img_data)).convert('RGB')
        except Exception as e:
            logging.error(f'Error loading {image_key}: {e}')
            image = Image.new('RGB', (224, 224), color='black')

        # Processor: tokenizer for text + processor for images
        # - Tokenize text and return token IDs
        # - Process images and return pixel values (3,224,224)
        inputs = self.processor(
            text=[desc], 
            images=image, 
            return_tensors='pt', 
            padding='max_length', 
            truncation=True
        )

        return {
            'token_ids': inputs['input_ids'].squeeze(0),
            'attention_mask': inputs['attention_mask'].squeeze(0),
            'pixel_values': inputs['pixel_values'].squeeze(0)
        }

def main():
    # Get train and validation data paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    train_csv_path = os.path.join(script_dir, "data_splits", "train.csv")
    val_csv_path = os.path.join(script_dir, "data_splits", "val.csv")

    # Create directory to save metadata of trained model
    run_dir = setup_experiment_dir(base_path=os.path.join(script_dir, 'trained_models/v1'))

    # Load model and processor
    logging.info(f"Loading model {CONFIG['model_id']} on {CONFIG['device']}...")
    model = CLIPModel.from_pretrained(CONFIG['model_id']).to(CONFIG['device'])
    processor = CLIPProcessor.from_pretrained(CONFIG['model_id'])
    
    # Prepare data
    logging.info("Preparing data...")
    train_dataset = SteamDatasetHF(train_csv_path, processor)
    val_dataset = SteamDatasetHF(val_csv_path, processor)

    train_loader = DataLoader(train_dataset, batch_size=CONFIG['batch_size'], shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=CONFIG['batch_size'], shuffle=False)

    # Optimizer
    optimizer = AdamW(model.parameters(), lr=CONFIG['learning_rate'], weight_decay=CONFIG['weight_decay'])

    # Training loop
    best_val_loss = float('inf')
    patience_counter = 0
    logging.info("Starting training...")
    for epoch in range(CONFIG['epochs']):
        model.train()
        total_train_loss = 0
        
        for batch in train_loader:
            # Move batch to device
            token_ids = batch['token_ids'].to(CONFIG['device'])
            attention_mask = batch['attention_mask'].to(CONFIG['device'])
            pixel_values = batch['pixel_values'].to(CONFIG['device'])

            optimizer.zero_grad()

            # Forward pass
            outputs = model(
                input_ids=token_ids,
                attention_mask=attention_mask,
                pixel_values=pixel_values,
                return_loss=True
            )

            # Backward pass
            loss = outputs.loss
            loss.backward()
            optimizer.step()

            total_train_loss += loss.item()
        
        avg_train_loss = total_train_loss / len(train_loader)

        # Validation
        model.eval()
        total_val_loss = 0
        with torch.no_grad():
            for batch in val_loader:
                token_ids = batch['token_ids'].to(CONFIG['device'])
                attention_mask = batch['attention_mask'].to(CONFIG['device'])
                pixel_values = batch['pixel_values'].to(CONFIG['device'])

                outputs = model(
                    input_ids=token_ids,
                    attention_mask=attention_mask,
                    pixel_values=pixel_values,
                    return_loss=True
                )

                total_val_loss += outputs.loss.item()
            
        avg_val_loss = total_val_loss / len(val_loader)
            
        logging.info(f"[Epoch {epoch+1}/{CONFIG['epochs']+1}] Train Loss: {avg_train_loss:.4f} | Val Loss: {avg_val_loss:.4f}")
                     
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
            
            if patience_counter >= CONFIG['patience']:
                logging.info("Stopped - Early stopping.")
                break

    logging.info(f"Training completed.")

if __name__ == '__main__':
    main()