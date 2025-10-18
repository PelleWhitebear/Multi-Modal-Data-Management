# Formatted Zone

### Documentation for the Formatted Zone.

The Formatted Zone is the second stage of our data pipeline, receiving data from the **Persistent Landing Zone**. Its sole purpose is **syntactic homogenization**: ensuring that all data of a specific type adheres to a single, canonical format.

This zone reads files from the `persistent_landing` bucket, performs format conversions (e.g., converting CSV/XML to JSON, or PNG to JPG), and stores the newly standardized files in the `formatted-zone` MinIO bucket, ready for the Trusted Zone.

## Content Table

-   [Structure](#structure)
-   [Naming Convention](#naming-convention)
-   [Available Scripts](#available-scripts)
    -   [`create.py`](#createpy)
    -   [`format_json.py`](#format_jsonpy)
    -   [`format_images.py`](#format_imagespy)
    -   [`format_videos.py`](#format_videospy)
    -   [`formatted_zone.sh`](#formatted_zonesh)

## Structure

The Formatted Zone is implemented as a single MinIO bucket with a structure that mirrors the Persistent Landing Zone, allowing for a clear and organized flow of data.

-   Primary bucket: `formatted-zone`
-   JSON sub-bucket: `json`
    -   Stores all textual data after it has been converted to the canonical JSON format.
-   Media sub-bucket: `media`
    -   Stores all image and video data after conversion to their respective canonical formats.

Example:

-   `formatted-zone`
    -   `json`
        -   `steam`
            -   `steam#20251010_202458#games.json`
        -   `steamspy`
            -   `steamspy#20251010_202503#games.json`
    -   `media`
        -   `image`
            -   `20251010_202458#060152#1.jpg`
            -   `20251010_202458#060152#2.jpg`
            ...
        -   `video`
            -   `20251010_202458#060152#1.mp4`
            -   `20251010_202458#060152#2.mp4`
            ...

## Naming Convention

The Formatted Zone **inherits the base naming convention** from the `persistent_landing` bucket to maintain versioning and traceability.

The only change is the **standardization of the file extension** to the chosen canonical format for each data type.

For JSON files (converted from CSV, XML, YAML, etc.):

`json/<data_source>/<data_source>#<YYYYMMDD_HHMMSS>#games.json`

For Image files (e.g., converted from PNG, HEIF):

`media/image/<YYYYMMDD_HHMMSS>#<game_identifier>#<number_of_media>.jpg`

For Video files (e.g., converted from AVI, MOV):

`media/video/<YYYYMMDD_HHMMSS>#<game_identifier>#<number_of_media>.mp4`

## Available Scripts

Scripts can be found under `formatted_zone/scripts`. These scripts handle pulling data from the persistent landing zone, transforming it, and loading it into the formatted zone.

### `create.py`

-   Generates the empty primary `formatted-zone` bucket and its main sub-buckets (`json`, `media`).

### `format_json.py`

-   Fetches the latest textual data (e.g., `steam` and `steamspy` files) from the persistent landing zone.
-   Checks if the file format is already JSON. If not (e.g., it's CSV, XML, or YAML), it converts the content into the canonical JSON format.
-   Compares the date of the landing zone file with the latest file in the formatted zone. If the landing zone file is newer, it replaces the old formatted file.

### `format_images.py`

-   Deletes all existing images from the `formatted-zone/media/image/` sub-bucket to prepare for a fresh update.
-   Fetches all images from the persistent landing zone's media/image folder.
-   Converts any images that are not in the target format (e.g., JPEG) and copies all others directly.

### `format_videos.py`

-   Deletes all existing videos from the `formatted-zone/media/video/` sub-bucket.
-   Fetches all videos from the persistent landing zone's media/video folder.
-   Converts any videos that are not in the target format (e.g., MP4) using `moviepy` and copies all others directly.

### `formatted_zone.sh`

-   An orchestration script that executes all Formatted Zone scripts sequentially:
    1.  `create.py`
    2.  `format_json.py`
    3.  `format_images.py`
    4.  `format_videos.py`
-   It also times each step and provides a total execution summary.