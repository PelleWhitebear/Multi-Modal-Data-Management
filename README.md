# ADSDB Project P2: Fine-Tuning

This repository contains the source code for part 2 of ADSDB project (fine-tuning). 

Part 1 implemented an end-to-end Data Warehouse ETL pipeline. 
**Part 2 (Current)** focuses on efficient model fine-tuning (LoRA, QLoRA) of a CLIP model to improve game description and retrieval performance.

## How to Run (Fine-Tuning Part 2)

### 1. Environment Setup

1.  Clone this repository.
2.  Place the provided `.env` file in the root directory.
3.  Build and start the Docker container:
    ```bash
    docker compose build
    docker compose up -d streamlit
    ```

### 2. Data Preparation (Skip Pipeline)

To save time, you do not need to run the full ETL pipeline from Part 1. We have provided the processed data.

1.  Go to MinIO: `http://localhost:9001` (Credentials in `.env`).
2.  Create a bucket named **`exploitation-zone`**.
3.  Unzip the provided data archive and upload the `json` and `media` folders into the `exploitation-zone` bucket.

### 3. Dataset Generation

Before training, you must prepare the dataset splits and apply data augmentation. Run the following commands from your terminal (these execute inside the docker container):

**Step A: Create Train/Val/Test Splits** This fetches data from the exploitation zone and organizes it into the training zone.
```bash
docker compose run pipeline python training_zone/prepare_dataset.py
```

**Step B: Data Augmentation** This applies transformations (rotation, noise, cropping) to the training images.
```bash
docker compose run pipeline python training_zone/data_augmentation.py
```

### 4. Fine-Tuning the Model
You can now fine-tune the CLIP model. Use the --technique flag to specify the method. Available techniques: fp32, fp16, lora, qlora.

Example - Train using QLoRA:
```bash
docker compose run pipeline python training_zone/fine_tune.py --technique qlora
```
The model checkpoints will be automatically saved to MinIO.

### 5. Running Experiments
After training, evaluate the model on the test set to obtain retrieval metrics (Recall@K, mAP, MRR).

Example - Evaluate QLoRA model:
```bash
docker compose run pipeline python training_zone/experiments.py --technique qlora
```

Baseline evaluation to evaluate the pre-trained CLIP model without fine-tuning:
```bash
docker compose run pipeline python training_zone/experiments.py --technique baseline
```

### 6. Cleanup
To stop the services:
```bash
docker compose down
```