import typer

from pgbackup.helpers import get_app_context, print_list, print_table

app = typer.Typer()


@app.command()
def read(server: str = typer.Option(default=None)):
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
