# Multi-Modal-Data-Management
Multi-Modal Data Management system for using data from Steam API's.

# How to run

```bash
# After any change:
docker compose build
```

```bash
# When need to run a .sh file:
docker compose run --rm pipeline /app/landing_zone/scripts/landing_zone.sh 
```

```bash
# When need to run a specific .py file:
docker compose run --rm pipeline python -m exploitation_zone.scripts.query # Notice it doesn't have the .py

```bash
# To turn everything off:
docker compose down
```