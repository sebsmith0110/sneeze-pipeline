import pandas as pd
from ecs_sneeze import fetch_rows_from_email
from s3io import append_sneeze_data
from fetch_weather_data import add_weather_data


def main(): 
    email_data = fetch_rows_from_email()
    if email_data is None or email_data.empty: 
        print("No new sneezes to record...")
        return

    data_to_add = add_weather_data(email_data)
    append_sneeze_data("seb-sneezeproject", data_to_add)

if __name__ == "__main__": 
    main()