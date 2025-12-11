import io
import boto3
import pandas as pd
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
FILENAME = "sneeze-data.csv"
SNEEZE_COLUMNS = [
    "Date","Time","Latitude","Longitude",
    "sneeze_datetime_utc","weather_hour_utc",
    "temperature_2m","relative_humidity_2m","apparent_temperature","surface_pressure",
    "cloud_cover","precipitation","rain","snowfall",
    "wind_speed_10m","wind_gusts_10m","wind_direction_10m",
    "shortwave_radiation","direct_radiation","diffuse_radiation","sunshine_duration",
    "pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","ozone","sulphur_dioxide","dust",
    "alder_pollen","birch_pollen","grass_pollen","mugwort_pollen","olive_pollen","ragweed_pollen",
    "carbon_dioxide","uv_index"
]

# Note that BytesIO is used so that data is stored in RAM, no temp files need to be created

def ensure_bucket(bucket_name):
    try: 
        s3.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Bucket {bucket_name} not found")
        else:
            raise

def read_sneeze_data(bucket_name): 
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=FILENAME)
        body = obj["Body"].read()
        df = pd.read_csv(io.BytesIO(body))
        print(f"Loaded {len(df)} rows from s3://{bucket_name}/{FILENAME}")
        return df
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("No sneeze_data.csv found yet — providing empty dataframe")
            return pd.DataFrame(columns=SNEEZE_COLUMNS)
        else:
            raise

def append_sneeze_data(bucket_name, df: pd.DataFrame): 
    # Can be an empty dataframe
    existing_df = read_sneeze_data(bucket_name)
    df_combined = pd.concat([existing_df, df], ignore_index=True)
    csv_buffer = io.BytesIO()
    df_combined.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3.put_object(Bucket=bucket_name, Key=FILENAME, Body=csv_buffer.getvalue())
    print(f"Appended {len(df)} rows (total {len(df_combined)}) to s3://{bucket_name}/{FILENAME}")

def dedupe_sneeze_data(bucket_name, subset=("Date", "Time"), keep="last"):
    """
    Remove duplicate rows from the sneeze CSV stored in S3, keeping the last entry
    for each Date/Time pair by default.
    """
    df = read_sneeze_data(bucket_name)
    if df.empty:
        print(f"No data found in s3://{bucket_name}/{FILENAME}, nothing to deduplicate.")
        return

    missing_cols = [col for col in subset if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Cannot deduplicate because columns {missing_cols} are missing")

    before = len(df)
    deduped = df.drop_duplicates(subset=list(subset), keep=keep).reset_index(drop=True)
    removed = before - len(deduped)

    if removed == 0:
        print(f"No duplicate rows detected for columns {subset}.")
        return

    csv_buffer = io.BytesIO()
    deduped.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    s3.put_object(Bucket=bucket_name, Key=FILENAME, Body=csv_buffer.getvalue())
    print(
        f"Removed {removed} duplicate rows (from {before} to {len(deduped)}) "
        f"based on columns {subset} in s3://{bucket_name}/{FILENAME}"
    )

