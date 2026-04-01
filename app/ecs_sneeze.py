import imaplib
import email
from email.header import decode_header
import pandas as pd
import email.utils
import os
import boto3
from botocore.exceptions import ClientError
import json
from s3io import read_sneeze_data

LAT_DEFAULT = 51.5198104503113
LONG_DEFAULT = -0.3083420544865619
BUCKET = "seb-sneezeproject"

def _get_gmail_credentials_from_secrets():
    # Need to set this as an env variable in the ECS task definition
    secret_arn = os.getenv("GMAIL_SECRET_ARN")
    sm = boto3.client("secretsmanager")
    try:
        resp = sm.get_secret_value(SecretId=secret_arn)
    except ClientError as e: 
        raise RuntimeError(f"Unable to retrieve secret {secret_arn}: {e}") from e
    
    if "SecretString" not in resp or not resp["SecretString"]:
        raise RuntimeError(f"Secret {secret_arn} does not contain a SecretString value")
    secret = json.loads(resp["SecretString"])

    username = secret.get("username")
    app_password = secret.get("app_password")
    return username, app_password


def _lines_to_rows(lines): 
    columns = ["Date","Time","Latitude","Longitude"]
    rows = []
    for raw in lines: 
        line = (
            raw.replace(" BST", "")
               .replace(" UTC", "")
               .replace(" GMT", "")
               .strip()
        )
        if not line:
            continue
        
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 2: 
            continue

        date_str = parts[0]
        time_str = parts[1]

        try: 
            lat_val = round(float(parts[2]), 4) if len(parts) > 2 else round(LAT_DEFAULT, 4)
            lon_val = round(float(parts[3]), 4) if len(parts) > 3 else round(LONG_DEFAULT, 4)
        except ValueError:
            lat_val, lon_val = round(LAT_DEFAULT, 4), round(LONG_DEFAULT, 4)
        rows.append([date_str, time_str, lat_val, lon_val])

    if not rows:
        return pd.DataFrame(columns=columns)
    
    df = pd.DataFrame(rows, columns=columns)
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce").dt.date
    df["Time"] = pd.to_datetime(df["Time"], format="%H:%M:%S", errors="coerce").dt.time
    df = df.dropna(subset=["Date","Time"])
    return df


def fetch_rows_from_email():
    current_data = read_sneeze_data(BUCKET)
    processed_dates = set()
    if not current_data.empty and "Date" in current_data:
        processed_dates = set(
            pd.to_datetime(current_data["Date"], errors="coerce").dt.date.dropna()
        )

    USERNAME, APP_PASSWORD = _get_gmail_credentials_from_secrets()
    try: 
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(USERNAME, APP_PASSWORD)
        mail.select("inbox")

        data_out = pd.DataFrame(columns=["Date","Time","Latitude","Longitude"])


        status, message_numbers = mail.search(None, 'Subject "Sneezes"')
        if status == "OK":
            ids = message_numbers[0].split()
            print(f"Found {len(ids)} emails")

            for msg_id in ids:
                status, msg_data = mail.fetch(msg_id, "(RFC822)")
                msg = email.message_from_bytes(msg_data[0][1])

                body_lines = []
                for part in msg.walk():
                    if part.get_content_type() != "text/plain":
                        continue
                    payload = part.get_payload(decode=True)
                    if payload is None:
                        continue
                    text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                    body_lines.extend(text.splitlines())

                df_rows = _lines_to_rows(body_lines)
                if not df_rows.empty:
                    df_rows = df_rows[~df_rows["Date"].isin(processed_dates)]
                    data_out = pd.concat([data_out, df_rows], ignore_index=True)
    finally:
        mail.close()
        mail.logout()

    return data_out
