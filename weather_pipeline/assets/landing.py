
import os
import requests
import pandas as pd
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
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": tz,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data["daily"]).rename(columns={"time": "date"})
    df.columns = [c.upper() for c in df.columns]
    if df.empty:
        return
    conn = engine.raw_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=os.getenv("WEATHER_TABLE", "RAW_WEATHER_FORECAST"),
            schema=os.getenv("WEATHER_SCHEMA", "RAW"),
            auto_create_table=True,
            overwrite=True,
        )
    finally:
        conn.close()
    if not success:
        raise RuntimeError("Failed to write data to Snowflake")
