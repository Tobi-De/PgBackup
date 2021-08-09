import datetime
import secrets
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, constr

from pgbackup.settings import settings


def localized_now():
    return datetime.datetime.now(tz=settings.tz_info)


Cron = constr(regex=r"/(\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*|(last)/", strip_whitespace=True)


class CronExpression(BaseModel):
    year: Optional[Cron] = None
    month: Optional[Cron] = None
    day: Optional[Cron] = None
    week: Optional[Cron] = None
    day_of_week: Optional[Cron] = None
    hour: Optional[Cron] = None
    minute: Optional[Cron] = None
    second: Optional[Cron] = None


class Backup(BaseModel):
    """A utility class to get backup infos from a filename and represent backup data"""

    servername: str
    database: str
    encrypted: bool = False
    created_at: datetime.datetime = Field(default_factory=localized_now)

    @property
    def filename(self) -> str:
        created_at = localized_now()
        filename = f"pgb-{self.servername}-{self.database}-{created_at.isoformat()}"
        return filename

    @classmethod
    def from_filename(cls, filename: str):
        is_a_pgb_backup = filename.startswith("pgb-")
        encrypted = filename.endswith(".gpg")
        if not is_a_pgb_backup:
            return
        name = Path(filename).stem
        _, servername, database, created_at = name.split("_")
        return cls(
            servername=servername,
            database=database,
            encrypted=encrypted,
            created_at=datetime.datetime.fromisoformat(created_at),
        )


class BaseResource(BaseModel):
    id: str = Field(default_factory=secrets.token_urlsafe)
    created_at: datetime.datetime = Field(default_factory=localized_now)


class BackupJob(BaseResource):
    schedule: CronExpression
    server_id: str
    database: str
    encrypt: bool = False
    next_run: Optional[datetime.datetime] = None
    last_run: Optional[datetime.datetime] = None


class Server(BaseResource):
    name: str
    host: str
    user: str
    password: str
    default_db: str

    def __str__(self):
        return self.name
