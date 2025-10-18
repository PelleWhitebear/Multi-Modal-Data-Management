# Multi-Modal-Data-Management
Multi-Modal Data Management system for using data from Steam API's.

# How to run

```bash
docker compose build # After any change
docker compose up -d minio # Starts MinIO instance.
docker compose run --rm pipeline /app/landing_zone/scripts/landing_zone.sh # This is the path inside the docker container.
docker compose run --rm pipeline python /app/landing_zone/scripts/delete.py # Notice the "python".
docker compose down # Turn everything off to try again.
```

The `docker compose run --rm pipeline` runs `bash` + whatever comes next, that is why I don't write bash /app/... in the first one but I od have to write python /app in the second one.  