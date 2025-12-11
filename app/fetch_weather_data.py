import sys
import pandas as pd
import numpy as np
import requests
from dateutil import tz
from typing import List, Tuple
import time

WEATHER_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
AIR_QUALITY_ENDPOINT = "https://air-quality-api.open-meteo.com/v1/air-quality"

WEATHER_VARS = [
    "temperature_2m", "relative_humidity_2m", "apparent_temperature",
    "surface_pressure", "cloud_cover", "precipitation", "rain", "snowfall",
    "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m", 
    "shortwave_radiation", "direct_radiation", "diffuse_radiation",
    "sunshine_duration"
]

AQ_VARS = [
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "ozone", "sulphur_dioxide", "dust", "alder_pollen", "birch_pollen",
    "grass_pollen", "mugwort_pollen", "olive_pollen", "ragweed_pollen", 
    "carbon_dioxide", "uv_index"
]

# Note that since timezone is not included in sneezes.csv, have to assume that in Britain
# Will determine whether in winter or summer daylight saving time with localize function
def parse_sneezes(df: pd.DataFrame) -> pd.DataFrame: 
    df = df.copy()
    date_parsed = pd.to_datetime(df['Date'], format="%Y-%m-%d", errors="raise")
    time_parsed = pd.to_datetime(df['Time'], format="%H:%M:%S", errors="raise")

    # Set timezone to Europe/London
    local_naive = pd.to_datetime(
        date_parsed.dt.strftime("%Y-%m-%d") + " " + time_parsed.dt.strftime("%H:%M:%S")
    )
    tzinfo = tz.gettz("Europe/London")
    local_aware = local_naive.dt.tz_localize(tzinfo, ambiguous="NaT", nonexistent="shift_forward")

    df["sneeze_datetime_utc"] = local_aware.dt.tz_convert("UTC")

    df["Latitude"] = pd.to_numeric(df["Latitude"], errors="raise")
    df["Longitude"] = pd.to_numeric(df["Longitude"], errors="raise")

    return df

# Rounding to NEAREST hour by default
def round_to_hour(ts: pd.Series, mode: str = "nearest") -> pd.Series:
    return ts.dt.round("h") if mode == "nearest" else ts.dt.floor("h")

# By grouping sneezes by lat/lon, can get all corresponding weather data in 1 API call
def daterange_for_group(df_group: pd.DataFrame, buffer_days: int) -> Tuple[str, str]: 
    min_dt = df_group["sneeze_datetime_utc"].min()
    max_dt = df_group["sneeze_datetime_utc"].max()
    start = (min_dt - pd.Timedelta(days=buffer_days)).date().isoformat()
    end = (max_dt + pd.Timedelta(days=buffer_days)).date().isoformat()
    return start, end

def http_get_with_retries(url: str, params: dict, attempts: int = 3, timeout: int = 60): 
    last_err = None
    for i in range(1, attempts + 1): 
        try: 
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e: 
            last_err = e
            if i < attempts: 
                time.sleep(1.5 * i)
            else: 
                raise last_err
            
def fetch_openmeteo_hourly(endpoint: str, lat: float, lon: float, start_date: str, 
                           end_date: str, variables: List[str]) -> pd.DataFrame: 
    params = { 
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date, 
        "end_date": end_date, 
        "hourly": ",".join(variables),
        "timezone": "UTC",
    }
    try: 
        resp = http_get_with_retries(endpoint, params)
    except Exception as e: 
        # return empty df when get requests are unsuccessful
        sys.stderr.write(f"[WARN] Fetch failed for {endpoint} @ ({lat},{lon} {start_date}->{end_date}: {e}\n")
        return pd.DataFrame(columns=["hour_utc"] + variables)
    
    data = resp.json()
    data
    if "hourly" not in data or "time" not in data["hourly"]: 
        sys.stderr.write(f"[WARN] No 'hourly/time' in response from {endpoint} @ ({lat},{lon})\n")
        return pd.DataFrame(columns=["hour_utc"] + variables)  
    
    hours = pd.to_datetime(pd.Series(data["hourly"]["time"]), utc=True)
    out = pd.DataFrame({"hour_utc": hours})
    for var in variables: 
        out[var] = data["hourly"].get(var, [np.nan])

    return out

def fetch_bundle(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame: 
    weather = fetch_openmeteo_hourly(WEATHER_ENDPOINT, lat, lon, start_date, end_date, WEATHER_VARS)
    aq = fetch_openmeteo_hourly(AIR_QUALITY_ENDPOINT, lat, lon, start_date, end_date, AQ_VARS)

    bundle = weather.merge(aq, on="hour_utc", how="outer")
    bundle["_lat"] = lat
    bundle["_lon"] = lon
    return bundle.sort_values("hour_utc").reset_index(drop=True)

def add_weather_data(df: pd.DataFrame): 
    sneezes = parse_sneezes(df)   
    sneezes["weather_hour_utc"] = round_to_hour(sneezes["sneeze_datetime_utc"], "nearest")
    merged_frames = []

    for (lat, lon), g in sneezes.groupby(['Latitude', 'Longitude']): 
        start, end = daterange_for_group(g, 1)
        bundle = fetch_bundle(lat, lon, start, end)

        # Merge onto group's sneeze rows
        g2 = g.merge(
            bundle, 
            left_on=["weather_hour_utc", "Latitude", "Longitude"],
            right_on=["hour_utc", "_lat", "_lon"],
            how="left"
        )
        g2 = g2.drop(columns=["_lat", "_lon", "hour_utc"])
        merged_frames.append(g2)

    merged = pd.concat(merged_frames, ignore_index=True)
    return merged