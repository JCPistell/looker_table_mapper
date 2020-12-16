import click
from looker_data_mapper import table_mapper


@click.command()
@click.option("-t", "--table", multiple=True, help="The table(s) we want to find")
@click.option("-g", "--git-url", help="The git url of the LookML repo")
@click.option("-l", "--looker-instance", help="The Looker instance to check")
@click.option("-i", "--ini-file", help="Path to the ini file to use for sdk authentication")
def mapper(**kwargs):
    table_mapper.main(**kwargs)
