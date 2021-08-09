import datetime
from pathlib import Path
from typing import Optional, IO
from tempfile import SpooledTemporaryFile

from apscheduler.jobstores.base import JobLookupError

from pgbackup.ressources import BackupJob, Server, context
from pgbackup.settings import settings
from pgbackup.utils import (
    compress_file,
    encrypt_file,
    setup_tmp_dir,
    uncompress_file,
    unencrypt_file,
    write_local_file,
)
from pgbackup.postgresql import PgDumpBinaryConnector, build_postgres_uri


def _remove_job_from_scheduler(job_id: str):
    try:
        context.scheduler.remove_job(job_id=job_id)
    except JobLookupError:
        pass


def _remove_job_from_context(job_id: str):
    try:
        context.backup_jobs.pop(job_id)
    except KeyError:
        pass
    else:
        context.save()


@setup_tmp_dir
def create_backup(
    connector: PgDumpBinaryConnector,
    filename: str,
    encrypt: bool = False,
    destination_folder: Optional[Path] = None,
) -> Path:
    outputfile = connector.create_dump()
    # compress dump file
    compressed_file, filename = compress_file(outputfile, filename)
    outputfile = compressed_file
    if encrypt:
        encrypted_file, filename = encrypt_file(outputfile, filename)
        outputfile = encrypted_file
    # Store backup
    outputfile.seek(0)
    if destination_folder is not None:
        return write_local_file(
            outputfile, filepath=destination_folder.joinpath(filename)
        )
    else:
        return context.storage.upload(
            file_path=write_local_file(
                outputfile, filepath=settings.TMP_DIR.joinpath(filename)
            )
        )


@setup_tmp_dir
def restore_backup(
    connector: PgDumpBinaryConnector,
    filename: str,
    decrypt: bool = False,
    passphrase: Optional[str] = None,
):
    input_file: IO = context.storage.download(filename=filename)
    if decrypt:
        unencrypted_file, input_filename = unencrypt_file(
            input_file=input_file, passphrase=passphrase
        )
        input_file.close()
        input_file = unencrypted_file
    uncompressed_file, input_filename = uncompress_file(
        input_file=input_file, filename=input_file.name
    )
    input_file.close()
    input_file = uncompressed_file
    input_file.seek(0)
    connector.restore_dump(input_file)


def run_backup_job(*, job_id: str):
    backup_job: Optional[BackupJob] = context.backup_jobs.get(job_id, None)
    if not backup_job:
        _remove_job_from_scheduler(job_id=job_id)
        return
    server: Optional[Server] = context.servers.get(backup_job.server_id, None)
    if server:
        connector = PgDumpBinaryConnector(
            postgres_uri=build_postgres_uri(server=server, database=backup_job.database)
        )
        create_backup(connector=connector)
        now = datetime.datetime.now(tz=settings.tz_info)
        backup_job.last_run = now
        backup_job.next_run = context.scheduler.get_job(
            job_id=backup_job.id
        ).next_run_time
        context.save()
    else:
        _remove_job_from_scheduler(job_id=job_id)
        _remove_job_from_context(job_id=job_id)
