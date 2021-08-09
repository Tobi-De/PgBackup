import typer

from pgbackup.ressources import Server, context

app = typer.Typer()


@app.command(name="list")
def read():
    """List all registered servers"""


@app.command()
def create(
    name: str = typer.Option(..., prompt=True),
    host: str = typer.Option(..., prompt=True),
    user: str = typer.Option(..., prompt=True),
    password: str = typer.Option(..., prompt=True, hide_input=True),
    default_db: str = typer.Option(..., prompt=True),
):
    """Add a new server configuration"""
    # name = name.strip().capitalize()
    # server = Server(
    #     name=name, host=host, user=user, password=password, default_db=default_db
    # )
    # if not server.pg_utils().is_connection_possible():
    #     typer.secho("Connection to the server failed", fg=typer.colors.RED)
    #     raise typer.Exit(1)
    # server_ = context.add_server(server=server)
    # if not server_:
    #     typer.echo(
    #         f"A server with the name {server.name} already exists",
    #     )
    #     raise typer.Exit(0)
    # typer.secho(f"server {server.name} added successfully", fg=typer.colors.GREEN)


@app.command()
def update(name: str = typer.Option(None)):
    """update server"""


@app.command()
def delete():
    """Remove a server from the available resources"""
    # context = get_app_context()
    # server = print_list_with_prompt(
    #     elements=context.servers.values(), confirmation_prompt=True
    # )
    # context.remove_server(server)
    # typer.secho(f"server {server.name} removed successfully", fg=typer.colors.GREEN)
