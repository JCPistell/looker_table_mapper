import click
from looker_data_mapper import table_mapper


@click.command()
@click.option("-t", "--table", multiple=True, help="The table(s) we want to find")
@click.option("-g", "--git-url", help="The git url of the LookML repo")
@click.option("-i", "--ini-file", help="Path to the ini file to use for sdk authentication")
@click.option("-l", "--looker-instance", help="In the ini file, which section to use (i.e. which Looker instance)")
def mapper(**kwargs):
    table_mapper.main(**kwargs)
