import typer
from apscheduler.schedulers.background import BackgroundScheduler

from pgbackup.core.config import settings
from pgbackup.core.constants import APSCHEDULER_JOBSTORES, StorageEngine
from pgbackup.core.resources import BackupJob
from pgbackup.helpers import (
    cron_prompt,
    get_app_context,
    print_list_with_prompt,
    run_backup_job,
)

app = typer.Typer()

scheduler = BackgroundScheduler(
    jobstores=APSCHEDULER_JOBSTORES, timezone=settings.tz_info
)


# scheduler.add_job(func=job.backup_db, trigger="cron", **job.schedule.dict(exclude_unset=True))


@app.command()
def read(
    server: str = typer.Option(default=None),
    db: str = typer.Option(default=None),
    storage_engine: StorageEngine = typer.Option(None),
):
    pass


@app.command()
def create(
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
def delete():
    pass
