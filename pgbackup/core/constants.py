from enum import Enum
from pathlib import Path

import typer
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

APP_NAME = "PgBackup"

APP_FOLDER = Path(typer.get_app_dir(APP_NAME))

RESOURCES_FILE_PATH = APP_FOLDER.joinpath("context.json")

SQLITE_JOBSTORE_URL = "sqlite:///" + str(APP_FOLDER) + "jobs.sqlite"

APSCHEDULER_JOBSTORES = {"default": SQLAlchemyJobStore(url=SQLITE_JOBSTORE_URL)}


class StorageEngine(str, Enum):
    S3 = "S3"
    LOCAL = "LOCAL"
