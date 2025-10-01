import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dagster import AssetExecutionContext, FreshnessPolicy, asset
from sqlalchemy import text

@asset(
    group_name="landing",
    compute_kind="python",
    required_resource_keys={"snowflake_sqlalchemy_engine"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),
)
def raw_boston_weather(context: AssetExecutionContext):
    eng = context.resources.snowflake_sqlalchemy_engine.get_engine()

    lat = float(os.getenv("WEATHER_LAT", "42.3601"))
    lon = float(os.getenv("WEATHER_LON", "-71.0589"))
    tz = os.getenv("WEATHER_TZ", "America/New_York")
    schema = os.getenv("WEATHER_SCHEMA", "RAW")
    table = os.getenv("WEATHER_TABLE", "RAW_WEATHER_FORECAST")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": tz,
        "forecast_days": 1,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    d = r.json()

    df = pd.DataFrame(d["daily"]).rename(columns={"time": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date

    today = datetime.now(ZoneInfo(tz)).date()
    df = df[df["date"] == today]
    if df.empty:
        context.log.info("No data for today; skipping.")
        return

    row = df.iloc[0]
    date_v = row["date"]
    weathercode_v = int(row["weathercode"])
    tmax_v = float(row["temperature_2m_max"])
    tmin_v = float(row["temperature_2m_min"])
    precip_v = float(row["precipitation_sum"])
    load_ts_v = datetime.now(timezone.utc)

    context.log.info(
        f"Prepared row for {today}: code={weathercode_v}, tmax={tmax_v}, tmin={tmin_v}, precip={precip_v}, load_ts_utc={load_ts_v.isoformat()}"
    )

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        DATE DATE,
        WEATHERCODE INTEGER,
        TEMPERATURE_2M_MAX FLOAT,
        TEMPERATURE_2M_MIN FLOAT,
        PRECIPITATION_SUM FLOAT,
        LOAD_TS_UTC TIMESTAMP_TZ
    )
    """
    merge_sql = f"""
    MERGE INTO {schema}.{table} t
    USING (
        SELECT
            :date_v::DATE        AS DATE,
            :weathercode_v::INT  AS WEATHERCODE,
            :tmax_v::FLOAT       AS TEMPERATURE_2M_MAX,
            :tmin_v::FLOAT       AS TEMPERATURE_2M_MIN,
            :precip_v::FLOAT     AS PRECIPITATION_SUM,
            :load_ts_v::TIMESTAMP_TZ AS LOAD_TS_UTC
    ) s
    ON t.DATE = s.DATE
    WHEN MATCHED THEN UPDATE SET
        WEATHERCODE = s.WEATHERCODE,
        TEMPERATURE_2M_MAX = s.TEMPERATURE_2M_MAX,
        TEMPERATURE_2M_MIN = s.TEMPERATURE_2M_MIN,
        PRECIPITATION_SUM = s.PRECIPITATION_SUM,
        LOAD_TS_UTC = s.LOAD_TS_UTC
    WHEN NOT MATCHED THEN INSERT (
        DATE, WEATHERCODE, TEMPERATURE_2M_MAX, TEMPERATURE_2M_MIN, PRECIPITATION_SUM, LOAD_TS_UTC
    ) VALUES (
        s.DATE, s.WEATHERCODE, s.TEMPERATURE_2M_MAX, s.TEMPERATURE_2M_MIN, s.PRECIPITATION_SUM, s.LOAD_TS_UTC
    )
    """

    with eng.begin() as cxn:
        cxn.execute(text(create_sql))
        cxn.execute(
            text(merge_sql),
            {
                "date_v": date_v,
                "weathercode_v": weathercode_v,
                "tmax_v": tmax_v,
                "tmin_v": tmin_v,
                "precip_v": precip_v,
                "load_ts_v": load_ts_v,
            },
        )

    context.log.info(f"Upserted 1 row into {schema}.{table}.")
