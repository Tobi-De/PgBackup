import typer

from .commands import backups, databases, jobs, servers

app = typer.Typer(name="pgbackup", help="Cli utility to schedule postgres backups")

app.add_typer(backups.app, name="backups")
app.add_typer(databases.app, name="databases")
app.add_typer(jobs.app, name="jobs")
app.add_typer(servers.app, name="servers")


@app.callback()
def main(show_config: bool = False):
    """
    A cli program to automate your postgresql databases backups..
    """


if __name__ == "__main__":
    app()
