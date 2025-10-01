
# Weather Pipeline (Dagster + Snowflake) — Proof of Concept

This repository demonstrates a simple, reusable pipeline that fetches daily weather data from the [Open-Meteo API](https://open-meteo.com/) and loads it into Snowflake using [Dagster](https://dagster.io/). It is designed as a **proof of concept**: anyone with their own Snowflake instance and key-pair authentication can run it.

## What it does

- Pulls a daily weather forecast for a configurable latitude/longitude.  
  *(By default, it points to Boston, MA near my favorite [café in Roslindale](https://www.google.com/search?q=square+root+roslindale+latitude+and+longitude))*  
- Loads the forecast into a Snowflake table (default: `RAW.RAW_WEATHER_FORECAST`).  
- Runs locally with a Dagster schedule, or automatically via GitHub Actions CI/CD.  

---

## Quickstart (GitHub Actions)

1. **Fork this repo** to your own GitHub account.  
2. **Add Snowflake credentials** as repo secrets:  
   - `SNOWFLAKE_ACCOUNT`  
   - `SNOWFLAKE_USER`  
   - `SNOWFLAKE_PRIVATE_KEY_B64`  
   - optional: `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`  
3. **Wait for the schedule** — the pipeline will run automatically at the configured times, loading data into your Snowflake table.

---

## Configure

### Weather settings (environment variables)
You can override defaults by setting these variables:
- `WEATHER_LAT` (default `42.3601`)
- `WEATHER_LON` (default `-71.0589`)
- `WEATHER_TZ` (default `America/New_York`)
- `WEATHER_SCHEMA` (default `RAW`)
- `WEATHER_TABLE` (default `RAW_WEATHER_FORECAST`)

### Snowflake credentials
Two options are supported:

**1. Local development (TOML file)**  
Create `~/.snowflake/connections.toml`:
```toml
[DEFAULT_CONNECTION]
account = "YOUR_ACCOUNT"
user = "YOUR_USER"
private_key_path = "/absolute/path/to/your_key.pem"
```

**2. CI/CD (GitHub Actions secrets)**  
Add these secrets in your repo settings:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PRIVATE_KEY_B64` (your PEM file base64-encoded)
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` *(optional, only if your PEM is encrypted)*

---

## Install

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

One-off run:
```bash
dagster asset materialize -m weather_pipeline --select raw_boston_weather
```

---

## CI/CD (GitHub Actions)

This repo includes two workflows:

- `.github/workflows/materialize_oct1_once.yml`  
  Runs **once** at 11:00 AM ET on Oct 1, 2025.

- `.github/workflows/materialize_daily.yml`  
  Runs **every day at 11:00 UTC** (~7:00 AM ET while DST is active).  
  Update to `12:00 UTC` after DST ends if you want to stay aligned with 7:00 AM local time.

Each workflow:
1. Installs dependencies.  
2. Runs the Dagster job:  
   ```bash
   dagster asset materialize -m weather_pipeline --select raw_boston_weather
   ```

No servers are required: GitHub’s runners execute the pipeline with your Snowflake secrets.

---

## Notes

- Obvious reminder (I hope!): Never commit private keys. 
- You can change the target database/schema/table by editing `weather_pipeline/__init__.py` or overriding via environment variables.  
- This project is intended as a **proof of concept** and starting point for more complex pipelines.
