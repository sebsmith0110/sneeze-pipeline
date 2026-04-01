# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an AWS ECS pipeline that collects personal sneezing data from Gmail, enriches it with weather/air quality data from Open-Meteo APIs, and stores it as a CSV in S3.

## Development Commands

```bash
# Install dependencies
pip install -r app/requirements.txt

# Run the pipeline locally (requires AWS env vars and GMAIL_SECRET_ARN)
cd app && python main.py

# Build Docker image
docker build -t seb-sneeze-project .

# Run container locally
docker run \
  -e GMAIL_SECRET_ARN=<arn> \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  seb-sneeze-project
```

## Architecture

The pipeline runs as a single-shot ECS task (no internal scheduler) with three stages chained in `app/main.py`:

1. **`ecs_sneeze.py`** — Connects to Gmail via IMAP, fetches emails with subject `"Sneezes"`, parses CSV rows (`Date, Time, Latitude, Longitude`), and deduplicates against existing S3 data. Gmail credentials are fetched from AWS Secrets Manager via `GMAIL_SECRET_ARN`.

2. **`fetch_weather_data.py`** — Enriches each sneeze with 14 weather variables and 15 air quality variables from the Open-Meteo archive/AQ APIs. Groups sneezes by lat/lon to minimise API calls. Sneezes are matched to the nearest UTC hour. Uses 3-attempt retry with exponential backoff; degrades gracefully on failure.

3. **`s3io.py`** — Reads/appends/writes the full dataset as `sneeze-data.csv` in S3 bucket `seb-sneezeproject`. Each append reads the existing CSV, concatenates new rows, and reuploads the entire file (BytesIO, no temp files).

### Timezone handling

Sneeze datetimes are naive and assumed to be in `Europe/London`. They are localised (DST-aware) and converted to UTC for all API calls and storage.

### Default coordinates

If an email row omits lat/lon, it defaults to `(51.5198, -0.3083)` (London).

## Deployment

Pushing to `main` triggers `.github/workflows/deploy.yml`, which:
1. Builds and pushes a Docker image to ECR (`seb_sneeze_project_ecr`, eu-west-1) tagged `{DATE}-{SHORT_SHA}` and `latest`
2. Registers a new ECS task definition revision for `seb-sneeze-task` pointing to the new image

Required GitHub Secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
