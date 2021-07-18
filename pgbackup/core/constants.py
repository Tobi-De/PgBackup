from enum import Enum
from pathlib import Path

import typer

APP_NAME = "PgBackup"

APP_FOLDER = Path(typer.get_app_dir(APP_NAME))

RESOURCES_FILE_PATH = APP_FOLDER.joinpath("context.json")


class StorageEngine(str, Enum):
    S3 = "S3"
    LOCAL = "LOCAL"
