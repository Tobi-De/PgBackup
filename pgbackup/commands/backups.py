import os
from pathlib import Path
from typing import List

import typer
from rich import Console

from pgbackup.core import create_backup
from pgbackup.postgresql import PgDumpBinaryConnector, build_postgres_uri
from pgbackup.ressources import Backup, Server, context

app = typer.Typer()
console = Console()


@app.command(name="list")
def read(
    servername: str = typer.Option(default=None),
    database: str = typer.Option(default=None),
):
    """List all available backups"""
    backup_list: List[Backup] = context.storage.backups()
    if servername:
        backup_list = list(filter(lambda b: b.servername == servername, backup_list))
    if database:
        backup_list = list(filter(lambda b: b.database == database, backup_list))
    # TODO print table


@app.command()
def create(
    servername: str = typer.Option(default=None),
    database: str = typer.Option(default=None),
    encrypt: bool = typer.Option(default=False),
    incwd: bool = typer.Option(default=False, prompt=True),
):
    # TODO: create a new instant backup
    #  list server if not provided, prompt for choice
    #  list database if not provided, prompt for choice
    server: Server
    connector = PgDumpBinaryConnector(
        build_postgres_uri(server=server, database=database)
    )  # noqa
    # TODO Test connection
    kwargs = {
        "connector": connector,
        "filename": Backup(
            database=database, servername=server.name, encrypted=encrypt
        ).filename,
        "encrypt": encrypt,
    }
    if incwd:
        kwargs.update({"destination_folder": Path(os.getcwd())})
    create_backup(**kwargs)


@app.command()
def download():
    """Download a backup to the current working directory"""


@app.command()
def restore(
    dest_db: str = typer.Option(
        None, help="The database where to restore the backup, must exists"
    )
):
    """Restore a backup from the available list"""
    # test connection to see if db backup exists


@app.command()
def delete():
    pass
