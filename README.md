# ADSDB Project: Multimodal Data Warehouse

This repository contains the source code for the Multimodal Data Warehouse Project from the Algorithms, Data Structures and Databases course (ADSDB-MDS). 

In this project, we implement an end-to-end Data Warehouse with its ETL pipeline code. In addition, we have also developed a Streamlit UI where the user can:

- Run the whole ETL pipeline.
- Run a specific part of the ETL pipeline.
- Perform same-modality and multimodal similarity search between text, image and video data.
- Chat with a Game Recommendation Assistant that will help the user find the next game to play from our catalog based on their preferences. (RAG)

## How to run

1. Initialize docker in your machine.

2. Run the following commands:
    ```bash
    docker compose build
    docker compose up -d streamlit
    ```

3. Go to ``http://localhost:8501`` and start playing around.

**Note that the pipeline has to be run in order to start using the similarity search / RAG tools.**