# Landing Zone

### Documentation for the Landing Zone.

The Landing Zone is the first stop for our data, where it is stored in its raw format. We implemented the Landing Zone using a MinIO bucket, dividing it into two different sub-buckets, the temporal and the persistent landing zones. The first one serves as temporary entry point for all our data from their respective data sources, while the second one stores everything classified per data type and source.

## Content Table

- [Structure](#structure)
- [Naming Convention](#naming-convention)
- [Available Scripts](#available-scripts)
  - [`create.py`](#createpy)
  - [`delete.py`](#deletepy)
  - [`ingest_games.py`](#ingest_gamespy)
  - [`ingest_media.py`](#ingest_mediapy)
  - [`move_to_persistent.py`](#move_to_persistentpy)
  - [`consts.py` and `utils.py`](#constspy-and-utilspy)


## Structure

The principal structure of the Landing Zone goes as follows:

- Primary bucket: `landing-zone` 
- Temporal sub-bucket: `temporal_landing`
  - Temporary stores new raw data inputs as they get loaded incrementally. We also keep backup files, created during the data loading that will get deleted when the final JSON files get moved to the persistent landing zone.
- Persistent sub-bucket: `persistent_landing`
  - Persistently archives all raw data inputs following preset naming conventions and classifications awaiting further processing.

Example:

- `landing-zone` 
  - `temporal_landing`
    - `steam_games.bak`
    - `steamspy_games.bak`
    - `steam_games.json`
    - `steamspy_games.json`
    - `060152_img_1.png`
    - `060152_vid_1.mp4`
  - `persistent_landing`
    - `json`
      - `steam`
        - `steam#20251010_202458#games.json`
      - `steamspy`
        - `steamspy#20251010_202503#games.json`
    - `media`
      - `images`
        - `20251010_202458#060152#1.png`
      - `videos`
        - `20251010_202458#060152#1.mp4`

## Naming Convention

When moving a file from the temporal to the persistent landing zone, we apply a set naming convention to include some metadata in order to them apart.

For JSON files, the naming convention is as follows:

`persistent_landing/json/<data_source>/<data_source>#<YYYYMMDD_HHMMSS>#games.json`


For media files, the naming convention is as follows:

`persistent_landing/media/<media_type>/<YYYYMMDD_HHMMSS>#<game_identifier>#<number_of_media>.png`

Examples:
```
JSON:
    persistent_landing/json/steamspy/steamspy#20251010_202503#games.json

IMAGE:
    persistent_landing/media/images/20251010_202458#060152#1.png
```

This is really helpful for versioning, tracing and further ingesting the data.


## Available Scripts

Scripts can be found under `landing_zone/scripts`. These cover, in a very modularly fashion, all available tasks in the Landing Zone.

### `create.py`
- Generates empty primary bucket and sub-buckets. 

### `delete.py`
- Deletes all buckets and elements inside them, since MinIO does not allow deletion in its Web UI.

### `ingest_games.py`
- Meant to be executed periodically, ingests a new and updated set of videogame data inputs, saving them in the temporal landing zone.

### `ingest_media.py`
- Meant to be executed periodically, ingests a new and updated set of media data inputs, saving them in the temporal landing zone.

### `move_to_persistent.py`
- Moves all elements inside the temporal landing zone to the persistent landing zone, deleting everything from the former when done.

### `consts.py` and `utils.py`
- `consts.py` contains constants and routes used in the rest of scripts.
- `utils.py` contains functions used in the rest of scripts.