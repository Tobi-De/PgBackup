from typing import List

import typer
from apscheduler.schedulers.background import BackgroundScheduler

from .core.config import settings
from .core.constants import StorageEngine
from .core.resources import Server, BackupJob, Backup
from .helpers import (
    get_app_context,
    print_list,
    print_list_with_prompt,
    print_table,
    run_backup_job,
    cron_prompt,
)
from .table import make_table
scheduler = BackgroundScheduler()


app = typer.Typer(name="pgBackup", help="Cli utility to schedule postgres backups")


# scheduler.add_job(func=job.backup_db, trigger="cron", **job.schedule.dict(exclude_unset=True))


# add-job, --server, --db
# remove-job
# list-jobs, --server, --db
# list-backups, --server, --db
# restore_backup , --des-db

# config-file
# set-max-keep


@app.command()
def add_server(
    name: str = typer.Option(..., prompt=True),
    host: str = typer.Option(..., prompt=True),
    user: str = typer.Option(..., prompt=True),
    password: str = typer.Option(..., prompt=True, hide_input=True),
    default_db: str = typer.Option(..., prompt=True),
):
    """Add a new server configuration"""
    context = get_app_context()
    name = name.strip().capitalize()
    server = Server(
        name=name, host=host, user=user, password=password, default_db=default_db
    )
    if not server.pg_utils().is_connection_possible():
        typer.secho("Connection to the server failed", fg=typer.colors.RED)
        raise typer.Exit(1)
    server_ = context.add_server(server=server)
    if not server_:
        typer.echo(
            f"A server with the name {server.name} already exists",
        )
        raise typer.Exit(0)
    typer.secho(f"server {server.name} added successfully", fg=typer.colors.GREEN)


@app.command()
def remove_server():
    """Remove a server from the available resources"""
    context = get_app_context()
    server = print_list_with_prompt(
        elements=context.servers.values(), confirmation_prompt=True
    )
    context.remove_server(server)
    typer.secho(f"server {server.name} removed successfully", fg=typer.colors.GREEN)


@app.command()
def servers():
    """List all registered servers"""
    context = get_app_context()
    print_list(elements=context.servers.values())


@app.command()
def databases(server: str = typer.Option(default=None)):
    """List all databases for all registered servers"""
    context = get_app_context()
    server = context.get_server_by_name(name=server)
    if server:
        print_list(elements=server.pg_utils().db_list())
        raise typer.Exit(0)
    labels = [server.name for server in context.servers.values()]
    if len(labels) == 0:
        typer.secho("No element to print", fg=typer.colors.CYAN)
        raise typer.Exit(0)
    columns = [server.pg_utils().db_list() for server in context.servers.values()]
    labels = [server.name for server in context.servers.values()]
    print_table(labels=labels, columns=columns)


@app.command()
def backups(
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
    table = make_table(labels=labels,rows=rows)
    typer.echo(table)
    #print_table(columns=columns, labels=labels)


@app.command()
def add_job(
    server: str = typer.Option(default=None),
    db: str = typer.Option(default=None),
    storage_engine: StorageEngine = typer.Option(default=settings.STORAGE_ENGINE),
):
    """Schedule a new backup job"""
    context = get_app_context()
    server = context.get_server_by_name(name=server)
    if server is None:
        server = print_list_with_prompt(elements=context.servers.values())
    db_list = server.pg_utils().db_list()
    if db not in db_list:
        db = print_list_with_prompt(elements=db_list)

    if not server.pg_utils(db_name=db).is_connection_possible():
        typer.secho("Connection to the database failed", fg=typer.colors.RED)
        raise typer.Exit(1)

    schedule = cron_prompt()
    job = BackupJob(
        server_id=server.id,
        db_name=db,
        schedule=schedule,
        storage_engine=storage_engine,
    )
    # add to aps scheduler
    job_ = context.add_job(job=job)
    if not job_:
        typer.secho(
            f"A backup job on the database {db} from the server {server.name} already exists"
        )
        raise typer.Exit(0)
    run_backup_job(backup_job_id=job.id)


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


@app.callback()
def main(show_config: bool = False):
    """
    Manage postgres backups in the awesome CLI app.
    """


if __name__ == "__main__":
    app()

"""TODO
- clean logger, choose when to use logger and when to user typer.echo
- write the remaining command and do some tests
- integrate apscheduler to the process
- add more docstrings
- publish 
- add email functionality
- write some test

"""
