import pandas as pd
from fetch_sneeze_data import fetch_sneeze_data_from_emails
from s3io import append_sneeze_data
from fetch_weather_data import add_weather_data


def main(): 
    email_data = fetch_sneeze_data_from_emails()
    if email_data is None or email_data.empty: 
        print("No new sneezes to record...")
        return

    data_to_add = add_weather_data(email_data)
    append_sneeze_data("seb-sneezeproject", data_to_add)

if __name__ == "__main__": 
    main()