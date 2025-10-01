import os
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import pandas as pd
from sqlalchemy import text
from dagster import asset, AssetExecutionContext, FreshnessPolicy
from snowflake.connector.pandas_tools import write_pandas

@asset(
    group_name="landing",
    compute_kind="python",
    required_resource_keys={"snowflake_sqlalchemy_engine"},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),
)
def raw_boston_weather(context: AssetExecutionContext):
    engine = context.resources.snowflake_sqlalchemy_engine.get_engine()

    lat = float(os.getenv("WEATHER_LAT", "42.3601"))
    lon = float(os.getenv("WEATHER_LON", "-71.0589"))
    tz = os.getenv("WEATHER_TZ", "America/New_York")
    schema = os.getenv("WEATHER_SCHEMA", "RAW")
    table = os.getenv("WEATHER_TABLE", "RAW_WEATHER_FORECAST")
    tmp = f"{table}_TMP"

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": tz,
        "forecast_days": 1,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data["daily"]).rename(columns={"time": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.date
    today_local = datetime.now(ZoneInfo(tz)).date()
    df = df[df["date"] == today_local]
    if df.empty:
        context.log.info("No data for today; skipping.")
        return

    df.columns = [c.upper() for c in df.columns]
    df["LOAD_TS_UTC"] = pd.Timestamp.utcnow().tz_localize("UTC")
    context.log.info(f"Prepared {len(df)} row(s) for {today_local}.")

    with engine.begin() as cxn:
        cxn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                DATE DATE,
                WEATHERCODE INTEGER,
                TEMPERATURE_2M_MAX FLOAT,
                TEMPERATURE_2M_MIN FLOAT,
                PRECIPITATION_SUM FLOAT,
                LOAD_TS_UTC TIMESTAMP_TZ
            )
        """))
        cxn.execute(text(f"""
            CREATE OR REPLACE TEMPORARY TABLE {schema}.{tmp} (
                DATE DATE,
                WEATHERCODE INTEGER,
                TEMPERATURE_2M_MAX FLOAT,
                TEMPERATURE_2M_MIN FLOAT,
                PRECIPITATION_SUM FLOAT,
                LOAD_TS_UTC TIMESTAMP_TZ
            )
        """))

    conn = engine.raw_connection()
    try:
        success, _, _, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=tmp,
            schema=schema,
            auto_create_table=False,
            overwrite=True,
        )
    finally:
        conn.close()
    if not success:
        raise RuntimeError("Failed to write to temp table")

    with engine.begin() as cxn:
        cxn.execute(text(f"""
            MERGE INTO {schema}.{table} t
            USING {schema}.{tmp} s
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
        """))

    context.log.info(f"Upserted 1 row into {schema}.{table}.")
