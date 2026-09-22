import pandas as pd

import app.main as main


# Bug 6: dedupe_sneeze_data existed in s3io.py but was never called from
# main(), so duplicate rows from retries/re-sent emails accumulated forever.
def test_main_dedupes_after_appending(monkeypatch):
    calls = []
    email_df = pd.DataFrame([{"Date": "2024-01-01"}])
    enriched_df = pd.DataFrame([{"Date": "2024-01-01", "weather_hour_utc": "x"}])

    monkeypatch.setattr(main, "fetch_sneeze_data_from_emails", lambda: email_df)
    monkeypatch.setattr(main, "add_weather_data", lambda df: enriched_df)
    monkeypatch.setattr(
        main, "append_sneeze_data", lambda bucket, df: calls.append(("append", bucket))
    )
    monkeypatch.setattr(
        main, "dedupe_sneeze_data", lambda bucket: calls.append(("dedupe", bucket)), raising=False
    )

    main.main()

    assert calls == [("append", "seb-sneezeproject"), ("dedupe", "seb-sneezeproject")]


def test_main_skips_append_and_dedupe_when_no_new_sneezes(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "fetch_sneeze_data_from_emails", lambda: pd.DataFrame())
    monkeypatch.setattr(
        main, "append_sneeze_data", lambda bucket, df: calls.append("append")
    )
    monkeypatch.setattr(
        main, "dedupe_sneeze_data", lambda bucket: calls.append("dedupe"), raising=False
    )

    main.main()

    assert calls == []
