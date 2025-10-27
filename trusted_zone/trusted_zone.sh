#!/bin/bash
set -e

echo "Starting Trusted Zone processing..."
create_t0=$(date +%s)
python3 -m trusted_zone.create
create_t1=$(date +%s)
echo "Trusted Zone buckets created."

echo "Processing JSON files..."
process_json_t0=$(date +%s)
python3 -m trusted_zone.process_json
process_json_t1=$(date +%s)
echo "JSON processing completed."

echo "Processing image files..."
process_images_t0=$(date +%s)
python3 -m trusted_zone.process_images
process_images_t1=$(date +%s)
echo "Image processing completed."

echo "Processing video files..."
process_videos_t0=$(date +%s)
python3 -m trusted_zone.process_videos
process_videos_t1=$(date +%s)
echo "Video processing completed."

echo "Trusted Zone processing completed."

elapsed=$(( process_videos_t1 - create_t0 ))
echo "Time taken for bucket creation: $((create_t1 - create_t0)) seconds."
echo "Time taken for JSON processing: $((process_json_t1 - process_json_t0)) seconds."
echo "Time taken for image processing: $((process_images_t1 - process_images_t0)) seconds."
echo "Time taken for video processing: $((process_videos_t1 - process_videos_t0)) seconds."
echo "Total time taken: $elapsed seconds."