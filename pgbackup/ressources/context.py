from json import JSONDecodeError
from typing import Dict, Optional

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from pydantic import BaseModel, Field

from pgbackup.settings import StorageEngine, settings
from pgbackup.storages import BaseStorage, LocalStorage, S3Storage

from .data import BackupJob, Server

ServerDict = Dict[str, Server]
BackupJobDict = Dict[str, BackupJob]


class AppContext(BaseModel):
    servers: ServerDict = Field(default_factory=dict)
    backup_jobs: BackupJobDict = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # create directory structure
        settings.APP_FOLDER.mkdir(parents=True, exist_ok=True)
        settings.local_backup_folder.mkdir(parents=True, exist_ok=True)

    @property
    def scheduler(self):
        return BackgroundScheduler(
            jobstores={"default": MemoryJobStore()}, timezone=settings.tz_info
        )

    @property
    def storage(self) -> BaseStorage:
        if settings.storage_engine == StorageEngine.LOCAL:
            storage_ = LocalStorage(backup_folder=settings.local_backup_folder)
        else:
            storage_ = S3Storage(
                bucket_name=settings.aws_bucket_name,
                bucket_path=settings.aws_bucket_path,
            )
        return storage_

    def get_server_by_name(self, name: str) -> Optional[Server]:
        for server in self.servers.values():
            if server.name == name:
                return server
        return

    def server_exists(self, server: Server) -> bool:
        for s in self.servers.values():
            if s == server:
                return True
        return False

    def job_exists(self, job: BackupJob) -> bool:
        for j in self.backup_jobs.values():
            if job.database == j.database and job.server_id == j.server_id:
                return True
        return False

    def save(self):
        with open(settings.RESOURCES_FILE_PATH, "w") as f:
            f.write(self.json())


def get_app_context() -> AppContext:
    try:
        ctx = AppContext.parse_file(settings.RESOURCES_FILE_PATH)
    except (FileNotFoundError, JSONDecodeError):
        ctx = AppContext()
    return ctx


context = get_app_context()
