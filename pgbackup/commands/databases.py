import typer

from pgbackup.ressources import context

app = typer.Typer()


@app.command(name="list")
def read(server: str = typer.Option(default=None)):
    """List all databases for all registered servers"""
    # server = context.get_server_by_name(name=server)
    # if server:
    #     # TODO print Table: print_list(elements=server.pg_utils().db_list())
    #     pass
    # labels = [server.name for server in context.servers.values()]
    # if len(labels) == 0:
    #     typer.secho("No element to print", fg=typer.colors.CYAN)
    #     raise typer.Exit(0)
    # columns = [server.pg_utils().db_list() for server in context.servers.values()]
    # labels = [server.name for server in context.servers.values()]
    # # TODO print table
