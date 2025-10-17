#!/bin/bash
set -e

echo "Starting landing zone ingestion process..."
create_t0=$(date +%s)
python3 landing_zone/scripts/create.py
create_t1=$(date +%s)
echo "Buckets and sub-buckets created."

echo "Ingesting game data from Steam and SteamSpy..."
ingest_games_t0=$(date +%s)
python3 landing_zone/scripts/ingest_games.py \
    --steam-outfile steam_games.json \
    --steamspy-outfile steamspy_games.json \
    --sleep 3 --retries 5 --autosave 10 --timeout 30
ingest_games_t1=$(date +%s)
echo "Game data ingestion completed."

echo "Ingesting media files..."
ingest_media_t0=$(date +%s)
python3 landing_zone/scripts/ingest_media.py --sleep 4 --timeout 5 --retries 5
ingest_media_t1=$(date +%s)
echo "Media files ingestion completed."

echo "Moving data to persistent storage..."
move_to_persistent_t0=$(date +%s)
python3 landing_zone/scripts/move_to_persistent.py
move_to_persistent_t1=$(date +%s)
echo "Landing zone ingestion process completed."

elapsed=$(( move_to_persistent_t1 - create_t0 ))
echo "Time taken for bucket creation: $((create_t1 - create_t0)) seconds."
echo "Time taken for game data ingestion: $((ingest_games_t1 - ingest_games_t0)) seconds."
echo "Time taken for media files ingestion: $((ingest_media_t1 - ingest_media_t0)) seconds."
echo "Time taken for moving data to persistent storage: $((move_to_persistent_t1 - move_to_persistent_t0)) seconds."
echo "Total time taken: $elapsed seconds."