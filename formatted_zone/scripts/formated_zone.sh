#!/bin/bash
set -e

echo "Starting Formatted Zone processing..."
create_t0=$(date +%s)
python3 create.py
create_t1=$(date +%s)
echo "Formatted Zone buckets created."

echo "Formatting JSON files..."
format_json_t0=$(date +%s)
python3 format_json.py
format_json_t1=$(date +%s)
echo "JSON formatting completed."

echo "Formatting image files..."
format_images_t0=$(date +%s)
python3 format_images.py
format_images_t1=$(date +%s)
echo "Image formatting completed."

echo "Formatting video files..."
format_videos_t0=$(date +%s)
python3 format_videos.py
format_videos_t1=$(date +%s)
echo "Video formatting completed."

echo "Formatted Zone processing completed."

elapsed=$(( format_videos_t1 - create_t0 ))
echo "Time taken for bucket creation: $((create_t1 - create_t0)) seconds."
echo "Time taken for JSON formatting: $((format_json_t1 - format_json_t0)) seconds."
echo "Time taken for image formatting: $((format_images_t1 - format_images_t0)) seconds."
echo "Time taken for video formatting: $((format_videos_t1 - format_videos_t0)) seconds."
echo "Total time taken: $elapsed seconds."