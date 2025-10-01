
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import requests
import pandas as pd
from dagster import asset, AssetExecutionContext, FreshnessPolicy

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
    table = os.getenv("WEATHER_TABLE", "WEATHER_TODAY")

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

    df["TEMP_MAX_F"] = df["temperature_2m_max"] * 9.0/5.0 + 32.0
    df["TEMP_MIN_F"] = df["temperature_2m_min"] * 9.0/5.0 + 32.0

    row = df.iloc[0]
    date_v = row["date"]
    weathercode_v = int(row["weathercode"])
    tmax_f_v = float(row["TEMP_MAX_F"])
    tmin_f_v = float(row["TEMP_MIN_F"])
    precip_v = float(row["precipitation_sum"])
    load_ts_v = datetime.now(timezone.utc)

    context.log.info(f"Prepared row: date={date_v}, code={weathercode_v}, tmax_f={tmax_f_v}, tmin_f={tmin_f_v}, precip_mm={precip_v}, load_ts_utc={load_ts_v.isoformat()}")

    from snowflake.connector.pandas_tools import write_pandas

    conn = eng.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                DATE DATE,
                WEATHERCODE INTEGER,
                TEMP_MAX_F FLOAT,
                TEMP_MIN_F FLOAT,
                PRECIPITATION_SUM FLOAT,
                LOAD_TS_UTC TIMESTAMP_TZ
            )
        """)
        cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {schema}.{table}_TMP LIKE {schema}.{table}")

        stage_df = pd.DataFrame([{
            "DATE": date_v,
            "WEATHERCODE": weathercode_v,
            "TEMP_MAX_F": tmax_f_v,
            "TEMP_MIN_F": tmin_f_v,
            "PRECIPITATION_SUM": precip_v,
            "LOAD_TS_UTC": load_ts_v,
        }])

        success, _, nrows, _ = write_pandas(
            conn=conn,
            df=stage_df,
            table_name=f"{table}_TMP",
            schema=schema,
            auto_create_table=False,
            overwrite=True,
            use_logical_type=True,
        )
        if not success or nrows != 1:
            raise RuntimeError("Failed to stage row")

        cur.execute(f"""
            MERGE INTO {schema}.{table} t
            USING {schema}.{table}_TMP s
            ON t.DATE = s.DATE
            WHEN MATCHED THEN UPDATE SET
              WEATHERCODE = s.WEATHERCODE,
              TEMP_MAX_F = s.TEMP_MAX_F,
              TEMP_MIN_F = s.TEMP_MIN_F,
              PRECIPITATION_SUM = s.PRECIPITATION_SUM,
              LOAD_TS_UTC = s.LOAD_TS_UTC
            WHEN NOT MATCHED THEN INSERT (
              DATE, WEATHERCODE, TEMP_MAX_F, TEMP_MIN_F, PRECIPITATION_SUM, LOAD_TS_UTC
            ) VALUES (
              s.DATE, s.WEATHERCODE, s.TEMP_MAX_F, s.TEMP_MIN_F, s.PRECIPITATION_SUM, s.LOAD_TS_UTC
            )
        """)
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}_TMP")
        conn.commit()
    finally:
        conn.close()

    context.log.info(f"Upserted 1 row into {schema}.{table}.")
