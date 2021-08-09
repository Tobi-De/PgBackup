from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
import pytz
import typer
from botocore.exceptions import ClientError
from pydantic import BaseSettings as PydanticBaseSettings
from pydantic import ValidationError, conint, validator
from pytz import UnknownTimeZoneError


class StorageEngine(str, Enum):
    S3 = "S3"
    LOCAL = "LOCAL"


class BaseSettings(PydanticBaseSettings):
    """Class to store pure configiguration that can be change with the <configure> command
    I'm not using BaseSetting from pydantic because I don't want to deal with environnment
    variables, and I'm NOT putting those directy into AppContext just to seprate pure
    configs with whay I'm calling ressources, servers and backup jobs, altough they all
    will be saved in the the config.json file

    """

    APP_NAME = "pgbackup"
    APP_FOLDER = Path(typer.get_app_dir(APP_NAME))
    TMP_DIR: Path = APP_FOLDER.joinpath("tmp")
    TMP_FILE_MAX_SIZE = 10 * 1024 * 1024
    TMP_FILE_READ_SIZE = 1024 * 1000
    RESOURCES_FILE_PATH = APP_FOLDER.joinpath("context.json")
    LOG_FILE = APP_FOLDER.joinpath("pgbackup.log")

    gpg_recipient: Optional[str] = None
    gpg_always_trust: bool = False
    keep_most_recent: conint(ge=1) = 5

    local_backup_folder: Optional[Path] = APP_FOLDER.joinpath("backups")
    aws_bucket_name: Optional[str] = None
    aws_bucket_path: Optional[str] = None

    s3_enabled: bool = False

    @validator("s3_enabled", pre=True)
    def get_s3_enabled(cls, _: bool) -> bool:
        try:
            boto3.client("s3")
        except ClientError:
            s3_enabled = False
        else:
            s3_enabled = True
        return s3_enabled

    storage_engine: StorageEngine = StorageEngine.LOCAL

    @validator("storage_engine")
    def validate_storage_engine(cls, v: StorageEngine, values: Dict[str, Any]):
        if v == StorageEngine.S3:
            assert values.get("s3_enabled"), "Invalid S3 configurations"
            assert bool(
                values.get("aws_bucket_name")
            ), "No bucket name set, set the AWS_BUCKET_NAME environnment variable"
        return v

    timezone: str = "UTC"

    @validator("timezone")
    def validate_timezone(cls, value):
        try:
            pytz.timezone(value)
        except UnknownTimeZoneError:
            raise ValidationError("Unknown timezone")
        return value

    @property
    def tz_info(self):
        return pytz.timezone(self.timezone)

    class Config:
        fields = {
            "timezone": {"env": "TIMEZONE"},
            "keep_most_recent": {"env": "KEEP_MOST_RECENT"},
            "storage_engine": {"env": "STORAGE_ENGINE"},
            "local_backup_folder": {"env": "LOCAL_BACKUP_FOLDER"},
            "aws_bucket_name": {"env": "AWS_BUCKET_NAME"},
            "aws_bucket_path": {"env": "AWS_BUCKET_PATH"},
            "gpg_recipient": {"env": "PGB_GPG_RECIPIENT"},
            "gpg_always_trust": {"env": "PGB_GPG_ALWAYS_TRUST"},
        }

        # env_file = ".env"


settings = BaseSettings()
