from typing import Any, Dict, Optional

import boto3
import pytz
from botocore.exceptions import ClientError
from pydantic import BaseSettings, ValidationError, conint, validator
from pytz import UnknownTimeZoneError

from .constants import APP_FOLDER, StorageEngine


class Settings(BaseSettings):
    """Class to store pure configiguration that can be change with the <configure> command
    I'm not using BaseSetting from pydantic because I don't want to deal with environnment
    variables, and I'm NOT putting those directy into AppContext just to seprate pure
    configs with whay I'm calling ressources, servers and backup jobs, altough they all
    will be saved in the the config.json file

    """

    LOCAL_BACKUP_FOLDER = APP_FOLDER.joinpath("backups")

    AWS_BUCKET_NAME: Optional[str] = None
    AWS_BUCKET_PATH: Optional[str] = None

    S3_ENABLED: bool = False

    @validator("S3_ENABLED", pre=True)
    def get_s3_enabled(cls, _: bool) -> bool:
        try:
            boto3.client("s3")
        except ClientError:
            s3_enabled = False
        else:
            s3_enabled = True
        return s3_enabled

    STORAGE_ENGINE: StorageEngine = StorageEngine.LOCAL

    @validator("STORAGE_ENGINE")
    def validate_storage_engine(cls, v: STORAGE_ENGINE, values: Dict[str, Any]):
        if v == StorageEngine.S3:
            assert values.get("S3_ENABLED"), "Invalid S3 configurations"
            assert bool(values.get("AWS_BUCKET_NAME")), (
                "No bucket name set, set the " "AWS_BUCKET_NAME environnment variable"
            )
        return v

    KEEY_MOST_RECENT: conint(ge=1) = 2
    TIMEZONE: str = "UTC"

    @validator("TIMEZONE")
    def validate_timezone(cls, value):
        try:
            pytz.timezone(value)
        except UnknownTimeZoneError:
            raise ValidationError("Unknown timezone")
        return value

    @property
    def tz_info(self):
        return pytz.timezone(self.TIMEZONE)

    class Config:
        """For development purpose"""

        # env_file = ".env"


settings = Settings()
