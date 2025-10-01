
# Weather Pipeline (Dagster + Snowflake) — Proof of Concept

This repository demonstrates a simple, reusable pipeline that fetches **today’s daily weather forecast** from the [Open-Meteo API](https://open-meteo.com/) and loads it into Snowflake using [Dagster](https://dagster.io/).

It is designed as a **proof of concept** that anyone can run in their own Snowflake account using key‑pair authentication.

---

## What it does (now)

- Pulls the **current day’s forecast** for a configurable latitude/longitude.
- Converts temperature values to **°Fahrenheit**.
- Upserts **exactly one row per day** into a Snowflake table (default: `RAW.WEATHER_TODAY`).  
- Adds a `LOAD_TS_UTC` timestamp showing when the row was loaded.
- Can run locally or on a schedule via GitHub Actions.

> Note: This pipeline ingests **forecasted** values (not real-time observations). For example, `TEMP_MAX_F` is today’s forecasted high temperature in °F.

---

## Quickstart (GitHub Actions)

1. **Fork this repo** to your GitHub account.  
2. **Add Snowflake credentials** as repo secrets:  
   - `SNOWFLAKE_ACCOUNT`  
   - `SNOWFLAKE_USER`  
   - `SNOWFLAKE_PRIVATE_KEY_B64`  
   - (optional) `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`  
3. The scheduled workflow will run and append one row each day.

---

## Configure

### Weather parameters (env overrides)
- `WEATHER_LAT` (default `42.3601`)
- `WEATHER_LON` (default `-71.0589`)
- `WEATHER_TZ` (default `America/New_York`)
- `WEATHER_SCHEMA` (default `RAW`)
- `WEATHER_TABLE` (default `WEATHER_TODAY`)  ← updated default

### Snowflake credentials
**Local (`~/.snowflake/connections.toml`)**
```toml
[DEFAULT_CONNECTION]
account = "YOUR_ACCOUNT"
user = "YOUR_USER"
private_key_path = "/absolute/path/to/your_key.pem"
```

**GitHub Actions (secrets)**
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PRIVATE_KEY_B64`
- (optional) `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`

---

## Install locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Run locally

Terminal A:
```bash
dagster dev -m weather_pipeline
```

Terminal B:
```bash
dagster-daemon run
```

One‑off materialization:
```bash
dagster asset materialize -m weather_pipeline --select raw_boston_weather
```

---

## CI/CD (GitHub Actions)

- `.github/workflows/materialize_oct1_once.yml` — one-off trigger at a specific time.
- `.github/workflows/materialize_daily.yml` — runs daily on a UTC schedule.

Each job installs dependencies and runs:
```bash
dagster asset materialize -m weather_pipeline --select raw_boston_weather
```

---

## Table schema

Default table: `RAW.WEATHER_TODAY`

| Column               | Type           | Meaning                                       |
|----------------------|----------------|-----------------------------------------------|
| `DATE`               | `DATE`         | Local date for the forecast (in `WEATHER_TZ`) |
| `WEATHERCODE`        | `INTEGER`      | Open‑Meteo weather code                        |
| `TEMP_MAX_F`         | `FLOAT`        | Forecasted high temp for today (°F)           |
| `TEMP_MIN_F`         | `FLOAT`        | Forecasted low temp for today (°F)            |
| `PRECIPITATION_SUM`  | `FLOAT`        | Forecasted precipitation total (mm)           |
| `LOAD_TS_UTC`        | `TIMESTAMP_TZ` | Load timestamp (UTC)                           |

---

## Notes

- This pipeline uses **forecast** data, not live observations. Differences from your current outside reading are expected.
- Never commit private keys. Use local secure files or GitHub Secrets.
- Override the table/database/schema with env vars to fit your environment.
