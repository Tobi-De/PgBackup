import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional

import boto3
from pydantic import BaseModel, PrivateAttr

from .resources import Backup


class BaseStorage(BaseModel, ABC):
    @abstractmethod
    def backup_list(self) -> List[Backup]:
        pass

    @abstractmethod
    def upload(self, file_path: Path):
        pass

    @abstractmethod
    def download(self, filename: str, dest_file: Path) -> Path:
        pass


class LocalStorage(BaseStorage):
    backup_folder: Path

    def backup_list(self) -> List[Backup]:
        backups = [
            Backup.from_filename(filename=f.name)
            for f in self.backup_folder.iterdir()
            if f.is_file()
        ]
        return list(filter(lambda x: bool(x), backups))

    def upload(self, file_path: Path):
        shutil.move(str(file_path), str(self.backup_folder))

    def download(self, filename: str, dest_file: Path) -> Path:
        shutil.move(filename, str(dest_file))
        return dest_file


class S3Storage(BaseStorage):
    bucket_name: str
    bucket_path: Optional[str] = None
    _s3_client: Any = PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._s3_client = boto3.client("s3")

    def backup_list(self) -> List[Backup]:
        kwargs = {"Bucket": self.bucket_name}
        if self.bucket_path:
            kwargs["Prefix"] = self.bucket_path
        s3_objects = self._s3_client.list_objects_v2(**kwargs)
        backups = [s3_content["Key"] for s3_content in s3_objects["Contents"]]
        backups = [Backup.from_filename(filename=f) for f in backups]
        return list(filter(lambda x: bool(x), backups))

    def upload(self, file_path: Path):
        dest_file = (
            self.bucket_path + "/" + file_path.name
            if self.bucket_path
            else file_path.name
        )
        self._s3_client.upload_file(
            str(file_path),
            self.bucket_name,
            dest_file,
        )
        file_path.unlink()

    def download(self, filename: str, dest_file: Path) -> Path:
        self._s3_client.download_file(
            self.bucket_name,
            str(self.bucket_path) + filename,
            str(dest_file),
        )
        return dest_file
