from typing import Dict, Optional

from pydantic import BaseModel, Field

from .config import settings
from .constants import APP_FOLDER, RESOURCES_FILE_PATH, StorageEngine
from .resources import BackupJob, Server
from .storages import BaseStorage, LocalStorage, S3Storage

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
        APP_FOLDER.mkdir(parents=True, exist_ok=True)
        settings.LOCAL_BACKUP_FOLDER.mkdir(parents=True, exist_ok=True)

    @property
    def storage(self) -> BaseStorage:
        if settings.STORAGE_ENGINE == StorageEngine.LOCAL:
            storage_ = LocalStorage(backup_folder=settings.LOCAL_BACKUP_FOLDER)
        else:
            storage_ = S3Storage(
                bucket_name=settings.AWS_BUCKET_NAME,
                bucket_path=settings.AWS_BUCKET_PATH,
            )
        return storage_

    def get_server_by_name(self, name: str) -> Optional[Server]:
        for server in self.servers.values():
            if server.name == name:
                return server

    def add_server(self, server: Server) -> Optional[Server]:
        for s in self.servers.values():
            if s.name == server.name:
                return
        self.servers[server.id] = server
        self.save()
        return server

    def remove_server(self, server: Server):
        self.servers.pop(server.id)
        self.save()

    def add_job(self, job: BackupJob) -> Optional[BackupJob]:
        for j in self.backup_jobs.values():
            if (
                job.db_name == j.db_name
                and job.server_id == j.server_id
                and job.storage_engine == j.storage_engine
            ):
                return
        self.backup_jobs[job.id] = job
        self.save()
        return job

    def save(self):
        with open(RESOURCES_FILE_PATH, "w") as f:
            f.write(self.json())
