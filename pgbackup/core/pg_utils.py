import gzip
import subprocess
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path
from typing import List

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pydantic import BaseModel, PostgresDsn

PgConnection = namedtuple("PgConnection", ["connection", "cursor"])


class PostgresRestoreError(Exception):
    pass


class PostgresBackupError(Exception):
    pass


@contextmanager
def managed_pg_connection(dsn: PostgresDsn, isolation_lvl: int = None):
    try:
        connection = psycopg2.connect(dsn=dsn)
        if isolation_lvl:
            connection.set_isolation_level(isolation_lvl)
        cursor = connection.cursor()
        yield PgConnection(connection=connection, cursor=cursor)
    finally:
        if connection:  # noqa
            cursor.close()  # noqa
            connection.close()  # noqa


class PostgresUtils(BaseModel):
    postgres_dsn: PostgresDsn

    def is_connection_possible(self) -> bool:
        try:
            psycopg2.connect(dsn=self.postgres_dsn)
        except psycopg2.Error:
            return False
        return True

    def db_list(self) -> List[str]:
        with managed_pg_connection(dsn=self.postgres_dsn) as pg_con:
            cursor = pg_con.cursor
            sql_query = "SELECT datname FROM pg_database"  # noqa
            cursor.execute(sql_query)
            result = cursor.fetchall()
            try:
                databases = [el[0] for el in result]
            except IndexError:
                databases = []
        return databases

    def create_db(self, db_name: str):
        # dsn example = 'postgresql://jean:password@localhost/postgres'
        user_name = self.postgres_dsn.split(":")[1].split("/")[-1]
        with managed_pg_connection(
            dsn=self.postgres_dsn, isolation_lvl=ISOLATION_LEVEL_AUTOCOMMIT
        ) as pg_con:
            cursor, con = pg_con.cursor, pg_con.connection
            stop_db_query = (
                f"SELECT pg_terminate_backend( pid ) FROM pg_stat_activity"  # noqa
                f" WHERE pid <> pg_backend_pid( ) AND datname = '{db_name}'"
            )
            drop_db_query = f"DROP DATABASE IF EXISTS {db_name};"
            create_db_query = f"CREATE DATABASE {db_name};"
            grant_privileges_query = (
                f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {user_name};"
            )
            cursor.execute(stop_db_query)
            cursor.execute(drop_db_query)
            cursor.execute(create_db_query)
            cursor.execute(grant_privileges_query)

    def restore_backup(self, backup_file: Path) -> str:
        """Restore a backup from a compress file. Few assumptions are done here
        - the postgres_dsn attribute contain the db name where to restore the backup
        - this database already exists on the server
        These are the steps to achive the restore:
        - extract the file
        - get the name of the database where to restore from the postres_dsn attribute
        - create a new database where the backup will be restored
        - stop the database provided in the postgres_dsn attribute
        - delete the database provided in the postgres_dsn if it exist
        - rename the restored database to the name of the database that was provided
        """
        ext_file = self.extract_file(backup_file)
        # dsn example = 'postgresql://jean:password@localhost/postgres'
        restored_db_name = self.postgres_dsn.split("/")[-1]
        restore_db_name = restored_db_name + "_restore"
        dsn_as_list = self.postgres_dsn.split("/")[:-1]
        restore_db_dsn = "/".join(dsn_as_list + [restore_db_name])
        self.create_db(db_name=restore_db_name)
        subprocess_params = [
            "pg_restore",
            "--no-owner",
            f"--dbname={restore_db_dsn}",
            str(backup_file),
        ]
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        process.communicate()
        if int(process.returncode) != 0:
            raise PostgresRestoreError(f"pg_restore failed on {self.postgres_dsn}")
        # swap after restore
        with managed_pg_connection(
            dsn=self.postgres_dsn, isolation_lvl=ISOLATION_LEVEL_AUTOCOMMIT
        ) as ctx:
            cursor = ctx[0]
            cursor.execute(
                "SELECT pg_terminate_backend( pid ) "  # noqa
                "FROM pg_stat_activity "
                "WHERE pid <> pg_backend_pid( ) "
                "AND datname = '{}'".format(restored_db_name)
            )
            cursor.execute("DROP DATABASE IF EXISTS {}".format(restored_db_name))
            cursor.execute(
                'ALTER DATABASE "{}" RENAME TO "{}";'.format(
                    restore_db_name, restored_db_name
                )
            )
        ext_file.unlink()
        return restored_db_name

    def create_backup(self, dest_file: Path) -> Path:
        """Create a new backup of the database and return the path to the compressed file. We assume
        that the <postgres_dsn> of the class is the url to the database we want to backup"""
        process = subprocess.Popen(
            [
                "pg_dump",
                f"--dbname={self.postgres_dsn}",
                "-Fc",
                "-f",
                str(dest_file),
                "-v",
            ],
            stdout=subprocess.PIPE,
        )
        process.communicate()
        if int(process.returncode) != 0:
            raise PostgresBackupError(f"pg_dump failed on {self.postgres_dsn}")
        compressed_file = self.compress_file(src_file=dest_file)
        dest_file.unlink()
        return compressed_file

    @classmethod
    def compress_file(cls, src_file: Path) -> Path:
        compressed_file = f"{str(src_file)}.gz"
        with open(src_file, "rb") as f_in:
            with gzip.open(compressed_file, "wb") as f_out:
                for line in f_in:
                    f_out.write(line)
        compressed_file_path = Path(compressed_file)
        return compressed_file_path

    @classmethod
    def extract_file(cls, src_file: Path) -> Path:
        extracted_file = src_file.stem
        with gzip.open(src_file, "rb") as f_in:
            with open(extracted_file, "wb") as f_out:
                for line in f_in:
                    f_out.write(line)
        extracted_file_path = Path(extracted_file)
        return extracted_file_path
