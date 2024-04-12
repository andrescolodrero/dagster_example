import os
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    FilesystemIOManager,  # Update the imports at the top of the file to also include this
    EnvVar,
    ConfigurableResource
)
from dagster_duckdb_pandas import DuckDBPandasIOManager
from .resources import DataGeneratorResource, MyConnectionStringResource, MyDatafilesPathResource
from . import assets, assets_FMR_data
from dotenv import load_dotenv

load_dotenv()

all_assets = load_assets_from_modules([assets, assets_FMR_data])

# Define a job that will materialize the assets
hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *",  # every hour
)

io_manager = FilesystemIOManager(
    base_dir="/dagster_home/data",  # Path is built relative to where `dagster dev` is run
)

database_io_manager = DuckDBPandasIOManager(
    database="/dagster_home/data/analytics.hackernews",
    )

datagen = DataGeneratorResource(
#    num_days=EnvVar.int("HACKERNEWS_NUM_DAYS_WINDOW"),
)

resources = {
    "local": {
        "io_manager": io_manager,
        "database_io_manager": database_io_manager,
        "hackernews_api": datagen,
        "publicdata_conn_string": MyConnectionStringResource(connection_string = r'Driver={ODBC Driver 18 for SQL Server};Server=mysqlserver;Database=PublicData;Trusted_Connection=yes;TrustServerCertificate=yes'),
        "publicdata_datafiles_path": MyDatafilesPathResource(datafiles_path = os.environ.get("DATAFILES_PATH"))
    },
    "testing": {
        "io_manager": io_manager,
        "database_io_manager": database_io_manager,
        "hackernews_api": datagen,
        "publicdata_conn_string": MyConnectionStringResource(connection_string = r'Driver={ODBC Driver 18 for SQL Server};Server=mysqlserver;Database=PublicData;Trusted_Connection=yes;TrustServerCertificate=yes'),
        "publicdata_datafiles_path": MyDatafilesPathResource(datafiles_path = os.environ.get("DATAFILES_PATH"))
    }
}

defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule],
    resources=resources["local"],
)
