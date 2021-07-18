from typing import List

import typer

from pgbackup.core.resources import Backup
from pgbackup.helpers import get_app_context, print_table

app = typer.Typer()


@app.command()
def read(
    server: str = typer.Option(default=None), db: str = typer.Option(default=None)
):
    """List all available backups"""
    context = get_app_context()
    backup_list: List[Backup] = context.storage.backup_list()
    if server:
        backup_list = list(filter(lambda b: b.server_name == server, backup_list))
    if db:
        backup_list = list(filter(lambda b: b.db_name == db, backup_list))
    labels = ["Server", "Database", "Created at"]
    rows = [list(backup.dict().values()) for backup in backup_list]
    print_table(rows=rows, labels=labels)


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
    context = get_app_context()
    # test connection to see if db backup exists


@app.command()
def delete():
    pass
