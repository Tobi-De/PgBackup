from typing import Any, List, Optional

import typer
from pydantic import StrRegexError
from rich import Console

from pgbackup.ressources.data import Cron, CronExpression

console = Console()


def _get_value(values: List[str], index: int) -> str:
    try:
        return values[index]
    except IndexError:
        return " "


def _get_row(columns: List[List[str]], index: int) -> List[str]:
    return [_get_value(column, index) for column in columns]


def columns_to_rows(columns: List[List[str]]) -> List[List[str]]:
    """Take a list of element disposed as list of columns and return a new list
    containing a list of rows
    """
    max_column_size = max([len(column) for column in columns])
    return [_get_row(columns, index) for index in range(max_column_size)]


def print_table(labels: List[str], rows: List[List[str]]):
    # TODO use generic to know the return type
    pass


def print_table_with_prompt(
    labels: List[str],
    rows: List[List[str]],
    choice_label: str,
    confirmation_prompt: bool = False,
) -> Optional[Any]:
    pass


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
