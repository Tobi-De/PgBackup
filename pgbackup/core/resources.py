import datetime
import secrets
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, PostgresDsn, constr

from .config import settings
from .constants import APP_FOLDER, StorageEngine
from .pg_utils import PostgresUtils

Cron = constr(regex=r"/(\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*|(last)/", strip_whitespace=True)


class Backup(BaseModel):
    server_name: str
    db_name: str
    created_at: Optional[datetime.datetime] = Field(
        default_factory=datetime.datetime.now
    )

    def __str__(self):
        return f"Server: {self.server_name} DB: {self.db_name} at {self.created_at.strftime('%Y-%m-%d %H:%M')}"

    @property
    def filename(self) -> Path:
        created_at = settings.tz_info.localize(self.created_at)
        filename = f"pgb_{self.server_name}_{self.db_name}_{created_at.isoformat()}.gz"
        return Path(filename)

    @classmethod
    def from_filename(cls, filename: str):
        is_a_pgb_backup = filename.startswith("pgb_") and filename.endswith(".gz")
        if not is_a_pgb_backup:
            return
        name = Path(filename).stem
        _, server_name, db_name, created_at = name.split("_")
        return cls(
            server_name=server_name,
            db_name=db_name,
            created_at=datetime.datetime.fromisoformat(created_at),
        )


class CronExpression(BaseModel):
    year: Optional[Cron] = None
    month: Optional[Cron] = None
    day: Optional[Cron] = None
    week: Optional[Cron] = None
    day_of_week: Optional[Cron] = None
    hour: Optional[Cron] = None
    minute: Optional[Cron] = None
    second: Optional[Cron] = None


class Resource(BaseModel):
    id: str = Field(default_factory=secrets.token_urlsafe)


class BackupJob(Resource):
    schedule: CronExpression
    server_id: str
    db_name: str
    storage_engine: StorageEngine
    apscheduler_job_id: Optional[str] = None
    last_run: Optional[datetime.datetime] = None
    first_run: Optional[datetime.datetime] = None

    def __str__(self):
        return f"job on {self.db_name}"


class Server(Resource):
    name: str
    host: str
    user: str
    password: str
    default_db: str

    def __str__(self):
        return self.name

    def dsn(self, db_name: str = None) -> PostgresDsn:
        return PostgresDsn.build(  # noqa
            scheme="postgresql",
            user=self.user,
            password=self.password,
            host=self.host,
            path=f"/{db_name or self.default_db}",
        )

    def pg_utils(self, db_name: Optional[str] = None) -> PostgresUtils:
        return PostgresUtils(postgres_dsn=self.dsn(db_name=db_name))

    def create_backup(self, db_name: str) -> Path:
        backup = Backup(server_name=self.name, db_name=db_name)
        # filename without extension, the compress method will add the extension
        dest_file = backup.filename.stem
        compressed_file = self.pg_utils(db_name=db_name).create_backup(
            dest_file=APP_FOLDER.joinpath(dest_file)
        )
        return compressed_file

    def restore_backup(self, backup_file: Path, dest_db_name: str = None) -> str:
        backup = Backup.from_filename(str(backup_file))
        dest_db_name = dest_db_name or backup.db_name
        restored_db_name = self.pg_utils(db_name=dest_db_name).restore_backup(
            backup_file=backup_file
        )
        return restored_db_name
