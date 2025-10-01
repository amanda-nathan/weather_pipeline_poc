
from dagster import Definitions, load_assets_from_modules, define_asset_job, ScheduleDefinition
from .assets import landing
from .resources.snowflake_io import snowflake_sqlalchemy_engine

all_assets = load_assets_from_modules([landing])
daily_weather_job = define_asset_job(name="daily_weather_job", selection=["raw_boston_weather"])
daily_schedule = ScheduleDefinition(job=daily_weather_job, cron_schedule="0 11 * * *", execution_timezone="America/New_York")

defs = Definitions(
    assets=all_assets,
    resources={
        "snowflake_sqlalchemy_engine": snowflake_sqlalchemy_engine(
            connection_name="DEFAULT_CONNECTION",
            warehouse="COMPUTE_WH",
            database="WEATHER_DB",
            schema_name="RAW",
        )
    },
    schedules=[daily_schedule],
)
