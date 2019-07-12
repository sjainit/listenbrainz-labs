import click
from prod.azure.create import create_cluster


cli = click.Group()


@cli.command(name="create_cluster")
@click.option('--nodes', '-n', default=4, type=int)
def create_cluster_command(nodes):
    create_cluster()


if __name__ == '__main__':
    cli()
