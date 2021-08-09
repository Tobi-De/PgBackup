import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional

import boto3
from pydantic import BaseModel, PrivateAttr

from .ressources.data import Backup


class BaseStorage(BaseModel, ABC):
    @abstractmethod
    def backups(self) -> List[Backup]:
        pass

    @abstractmethod
    def upload(self, file_path: Path) -> Path:
        pass

    @abstractmethod
    def download(self, filename: str, dest_file: Path) -> Path:
        pass

    @abstractmethod
    def clean_old_backups(self) -> None:
        pass

    @abstractmethod
    def delete_backup(self) -> str:
        pass


class LocalStorage(BaseStorage):
    backup_folder: Path

    def backups(self) -> List[Backup]:
        backups = [
            Backup.from_filename(filename=f.name)
            for f in self.backup_folder.iterdir()
            if f.is_file()
        ]
        return list(filter(lambda x: bool(x), backups))

    def upload(self, file_path: Path) -> Path:
        shutil.move(str(file_path), str(self.backup_folder))
        return self.backup_folder.joinpath(file_path.name)

    def download(self, filename: str, dest_file: Path) -> Path:
        shutil.move(filename, str(dest_file))
        return dest_file

    def delete_backup(self) -> str:
        pass

    def clean_old_backups(self) -> None:
        pass


class S3Storage(BaseStorage):
    bucket_name: str
    bucket_path: Optional[str] = None
    _s3_client: Any = PrivateAttr()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._s3_client = boto3.client("s3")

    def backups(self) -> List[Backup]:
        kwargs = {"Bucket": self.bucket_name}
        if self.bucket_path:
            kwargs["Prefix"] = self.bucket_path
        s3_objects = self._s3_client.list_objects_v2(**kwargs)
        backups = [s3_content["Key"] for s3_content in s3_objects["Contents"]]
        backups = [Backup.from_filename(filename=f) for f in backups]
        return list(filter(lambda x: bool(x), backups))

    def upload(self, file_path: Path) -> Path:
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
        return Path(f"S3:{self.bucket_name}/{dest_file}")

    def delete_backup(self) -> str:
        pass

    def download(self, filename: str, dest_file: Path) -> Path:
        self._s3_client.download_file(
            self.bucket_name,
            str(self.bucket_path) + filename,
            str(dest_file),
        )
        return dest_file

    def clean_old_backups(self) -> None:
        pass
