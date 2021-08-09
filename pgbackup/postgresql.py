import logging
import shlex
from contextlib import contextmanager
from dataclasses import dataclass
from subprocess import Popen
from tempfile import SpooledTemporaryFile
from typing import IO, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import connection as Connection
from psycopg2.extensions import cursor as Cursor
from pydantic import PostgresDsn

from pgbackup.exceptions import CommandConnectorError
from pgbackup.ressources import Server
from pgbackup.settings import settings

logger = logging.getLogger("dbbackup.command")
logger.setLevel(logging.DEBUG)


@dataclass
class PgConnection:
    connection: Connection
    cursor: Cursor


def build_postgres_uri(server: Server, database: str) -> str:
    return PostgresDsn.build(
        scheme="postgresql",
        user=server.user,
        password=server.password,
        host=server.host,
        path=f"/{database or server.default_db}",
    )


@contextmanager
def managed_pg_connection(dsn: str, isolation_lvl: Optional[int] = None):
    connection: Connection = psycopg2.connect(dsn=dsn)
    if isolation_lvl:
        connection.set_isolation_level(isolation_lvl)
    try:
        cursor: Cursor = connection.cursor()
        yield PgConnection(connection, cursor)
    finally:
        connection.close()


def run_command(
    command: str, stdin: Optional[IO] = None
) -> Tuple[SpooledTemporaryFile, SpooledTemporaryFile]:
    """
    Launch a shell command line.
    """
    logger.debug(command)
    cmd = shlex.split(command)
    stdout = SpooledTemporaryFile(
        max_size=settings.TMP_FILE_MAX_SIZE, dir=settings.TMP_DIR
    )
    stderr = SpooledTemporaryFile(
        max_size=settings.TMP_FILE_MAX_SIZE, dir=settings.TMP_DIR
    )
    try:
        if stdin is not None:
            with open(stdin, "rb") as input_file:
                process = Popen(
                    cmd,
                    stdin=input_file,
                    stdout=stdout,
                    stderr=stderr,
                )
        else:
            process = Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr)
        process.wait()
        if process.poll():
            stderr.seek(0)
            raise CommandConnectorError(
                "Error running: {}\n{}".format(command, stderr.read().decode("utf-8"))
            )
        stdout.seek(0)
        stderr.seek(0)
        return stdout, stderr
    except OSError as err:
        raise CommandConnectorError("Error running: {}\n{}".format(command, str(err)))


class PgDumpBinaryConnector:
    """
    PostgreSQL connector, it uses pg_dump`` to create an SQL text file
    and ``pg_restore`` for restore it.
    """

    extension: str = "psql.bin"
    dump_cmd: str = "pg_dump"
    restore_cmd: str = "pg_restore"
    single_transaction: bool = True
    drop: bool = True

    def __init__(self, postgres_uri: str):
        self.postgres_uri = postgres_uri

    def databases(self) -> List[str]:
        with managed_pg_connection(dsn=self.postgres_uri) as pg_con:
            cursor = pg_con.cursor
            sql_query = "SELECT datname FROM pg_database"
            cursor.execute(sql_query)
            result = cursor.fetchall()
            try:
                databases = [el[0] for el in result]
            except IndexError:
                databases = []
        return databases

    def create_database(self, db_name: str):
        user_name = self.postgres_uri.split(":")[1].split("/")[-1]
        with managed_pg_connection(
            dsn=self.postgres_uri, isolation_lvl=ISOLATION_LEVEL_AUTOCOMMIT
        ) as pg_con:
            cursor, con = pg_con.cursor, pg_con.connection
            stop_db_query = (
                f"SELECT pg_terminate_backend( pid ) FROM pg_stat_activity"
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

    def create_dump(self) -> SpooledTemporaryFile:
        cmd = f"{self.dump_cmd} {self.postgres_uri} --format=custom"
        cmd = "{} {} {}".format("", cmd, "")
        stdout, _ = run_command(cmd)
        return stdout

    def restore_dump(
        self, dump: SpooledTemporaryFile
    ) -> Tuple[SpooledTemporaryFile, SpooledTemporaryFile]:
        cmd = f"{self.restore_cmd} {self.postgres_uri}"

        if self.single_transaction:
            cmd += " --single-transaction"
        if self.drop:
            cmd += " --clean"
        stdout, stderr = run_command(cmd, stdin=dump)
        return stdout, stderr
