import pandas as pd

import app.fetch_weather_data as fwd


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def test_round_to_hour_nearest_rounds_up_past_half_hour():
    ts = pd.Series([pd.Timestamp("2024-06-15 23:45:00", tz="UTC")])
    rounded = fwd.round_to_hour(ts, "nearest")
    assert rounded.iloc[0] == pd.Timestamp("2024-06-16 00:00:00", tz="UTC")


def test_round_to_hour_floor_never_rounds_forward():
    ts = pd.Series([pd.Timestamp("2024-06-15 23:45:00", tz="UTC")])
    rounded = fwd.round_to_hour(ts, "floor")
    assert rounded.iloc[0] == pd.Timestamp("2024-06-15 23:00:00", tz="UTC")


def test_parse_sneezes_marks_dst_fallback_hour_as_nat():
    # 2024-10-27 01:30 local is in the UK clocks-back window and has no
    # unambiguous UTC mapping.
    df = pd.DataFrame(
        [{"Date": "2024-10-27", "Time": "01:30:00", "Latitude": "51.5", "Longitude": "-0.1"}]
    )
    result = fwd.parse_sneezes(df)
    assert pd.isna(result.loc[0, "sneeze_datetime_utc"])


# ---------------------------------------------------------------------------
# Bug 3: NaT crash on DST fallback — daterange_for_group must not blow up
# when every sneeze in a location group is NaT, and add_weather_data must
# skip that group instead of crashing the whole pipeline run.
# ---------------------------------------------------------------------------

def test_daterange_for_group_returns_none_when_all_nat():
    df_group = pd.DataFrame({"sneeze_datetime_utc": [pd.NaT, pd.NaT]})
    start, end = fwd.daterange_for_group(df_group, buffer_days=1)
    assert start is None
    assert end is None


def test_add_weather_data_skips_group_with_all_nat_instead_of_crashing(monkeypatch):
    df = pd.DataFrame(
        [{"Date": "2024-10-27", "Time": "01:30:00", "Latitude": 51.5, "Longitude": -0.1}]
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("fetch_bundle should not be called for an all-NaT group")

    monkeypatch.setattr(fwd, "fetch_bundle", fail_if_called)

    result = fwd.add_weather_data(df)
    assert len(result) == 1
    assert pd.isna(result.loc[0, "sneeze_datetime_utc"])


# ---------------------------------------------------------------------------
# Bug 2 + 5: rounding "nearest" could push weather_hour_utc past the date
# range that was actually requested/available, silently producing NaN
# weather columns for the affected sneeze.
# ---------------------------------------------------------------------------

def test_add_weather_data_keeps_weather_hour_within_same_day(monkeypatch):
    # January -> GMT (UTC+0), so local time == UTC time and the 23:45 case
    # actually exercises the midnight-rollover scenario from the bug report.
    df = pd.DataFrame(
        [{"Date": "2024-01-15", "Time": "23:45:00", "Latitude": 51.5, "Longitude": -0.1}]
    )

    def fake_fetch_bundle(lat, lon, start_date, end_date):
        row = {v: 1.0 for v in fwd.WEATHER_VARS + fwd.AQ_VARS}
        row.update(
            {"hour_utc": pd.Timestamp("2024-01-15 23:00:00", tz="UTC"), "_lat": lat, "_lon": lon}
        )
        return pd.DataFrame([row])

    monkeypatch.setattr(fwd, "fetch_bundle", fake_fetch_bundle)

    result = fwd.add_weather_data(df)

    assert result.loc[0, "weather_hour_utc"] <= pd.Timestamp("2024-01-15 23:59:59", tz="UTC")
    assert not pd.isna(result.loc[0, "temperature_2m"])


# ---------------------------------------------------------------------------
# Bug 8: date.today() resolves to the container's local date, not UTC. This
# can't be exercised realistically with freezegun's tz_offset (it shifts
# datetime.now(timezone.utc) too, not just naive local time), so instead we
# directly control what "now" resolves to and check the cap uses it.
# ---------------------------------------------------------------------------

def test_daterange_for_group_caps_end_using_injected_utc_now(monkeypatch):
    class FixedDateTime:
        @staticmethod
        def now(tz=None):
            return pd.Timestamp("2024-06-15 23:30:00", tz="UTC").to_pydatetime()

    monkeypatch.setattr(fwd, "datetime", FixedDateTime)

    group = pd.DataFrame({"sneeze_datetime_utc": [pd.Timestamp("2024-06-20 01:00:00", tz="UTC")]})
    start, end = fwd.daterange_for_group(group, buffer_days=1)
    assert end == "2024-06-15"


# ---------------------------------------------------------------------------
# Bug 9: unclosed parenthesis in the WARN f-string made the log line
# unparseable by anything grepping for "(lat,lon)".
# ---------------------------------------------------------------------------

def test_fetch_openmeteo_hourly_warns_with_correctly_matched_parens(monkeypatch, capsys):
    def raise_error(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(fwd, "http_get_with_retries", raise_error)

    fwd.fetch_openmeteo_hourly(
        fwd.WEATHER_ENDPOINT, 51.5, -0.1, "2024-06-01", "2024-06-02", fwd.WEATHER_VARS
    )

    captured = capsys.readouterr()
    assert "(51.5,-0.1)" in captured.err
