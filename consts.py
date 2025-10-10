# constants.py
from datetime import timezone

# # === MinIO connection ===
# MINIO_ENDPOINT = "http://localhost:9000"
# MINIO_ACCESS_KEY = "ROOTNAME"
# MINIO_SECRET_KEY = "CHANGEME123"

# === Landing Zone structure ===
LANDING_ZONE_BUCKET = "landing-zone"
TEMPORAL_SUB_BUCKET = "temporal_landing"
PERSISTENT_SUB_BUCKET = "persistent_landing"

# === Data source info ===
DATA_SOURCE = "steam_api"
DATA_FOLDER = "./data"

# === Timestamp format ===
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
TIMEZONE = timezone.utc
