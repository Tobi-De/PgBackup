import datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Iterable, List, Optional

import typer
from pydantic import StrRegexError

from .core.config import settings
from .core.constants import RESOURCES_FILE_PATH
from .core.context import AppContext
from .core.resources import BackupJob, Cron, CronExpression, Server
from .table import make_table

PLACEHOLDER = "?"


def _get_value(values: List[str], index: int):
    try:
        return values[index]
    except IndexError:
        return " "


def _get_row(columns: List[List[str]], index: int):
    return [_get_value(column, index) for column in columns]


def get_app_context() -> AppContext:
    try:
        ctx = AppContext.parse_file(RESOURCES_FILE_PATH)
    except (FileNotFoundError, JSONDecodeError):
        ctx = AppContext()
    return ctx


def run_backup_job(backup_job_id: str):
    context = get_app_context()
    backup_job: BackupJob = context.backup_jobs.get(backup_job_id, None)
    if not backup_job:
        # remove from apsschelduer
        return
    server: Server = context.servers.get(backup_job.server_id, None)
    if server:
        backup_file: Path = server.create_backup(
            db_name=backup_job.db_name,
        )
        context.storage.upload(file_path=backup_file)

        now = datetime.datetime.now(tz=settings.tz_info)
        backup_job.last_run = now
        if not backup_job.first_run:
            backup_job.first_run = now
        context.save()
    else:
        # remove job from apsschelduer
        # remove job from context
        pass


def columns_to_rows(columns: List[List[str]]) -> List[List[str]]:
    """Take a list of element disposed as list of columns and return a new list
    containing a list of rows
    """
    max_column_size = max([len(column) for column in columns])
    return [_get_row(columns, index) for index in range(max_column_size)]


def print_table(labels: List[str], rows: List[List[str]]):
    """
    :param labels: a list os string that represent the header of the table
    :param rows: a list of all columns, each column associated to a label at the same index
    """
    if len(rows) == 0:
        typer.secho("No elements to print", fg=typer.colors.CYAN)
        raise typer.Exit(0)
    table = make_table(rows=rows, labels=labels)
    # table = table.replace(PLACEHOLDER, " ")
    typer.echo(table)


def print_list(elements: Iterable[Any]):
    values = list(elements)
    if len(values) == 0:
        typer.secho("No elements to print", fg=typer.colors.CYAN)
        raise typer.Exit(0)

    def render_index(val):
        return "[" + typer.style(str(val), fg=typer.colors.GREEN) + "]"

    def render_value(val):
        return typer.style(str(val), fg=typer.colors.BLUE)

    content = [
        f"{render_index(index + 1)} {render_value(value)}"
        for index, value in enumerate(values)
    ]
    message = "\n".join(content)
    typer.secho(message=message)


def print_list_with_prompt(
    elements: Iterable, confirmation_prompt: bool = False
) -> Optional[Any]:
    values = list(elements)
    print_list(values)
    try:
        choice = typer.prompt(
            typer.style("choice", fg=typer.colors.CYAN),
            type=int,
            default=1,
            confirmation_prompt=confirmation_prompt,
        )
        assert choice in range(1, len(values) + 1)
    except AssertionError:
        typer.secho("Invalid choice", fg=typer.colors.RED)
        raise typer.Exit(0)
    return values[choice - 1]


def cron_prompt() -> Optional[CronExpression]:
    skip = "skip"
    fields = ["year", "month", "day", "week", "day_of_week", "hour", "minute", "second"]
    data = {}
    typer.secho(
        f"Specify your schedule below, this fields correspond to cron elements, press enter to skip.",
        fg=typer.colors.CYAN,
    )

    for field in fields:
        field_ = field.replace("_", " ").capitalize()
        val = typer.prompt(field_, default=skip)
        try:
            if val != skip:
                Cron.validate(val)
        except StrRegexError:
            typer.secho(f"Invalid {field_}")
            raise typer.Exit(1)
        else:
            if val and val != skip:
                data[field] = val
    cron = CronExpression(**data)
    return cron
