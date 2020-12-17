![badge](https://github.com/JCPistell/looker_table_mapper/workflows/python_application/badge.svg)

# Looker Data Mapper

This tool is used to map database tables to their derived content in Looker. You pass in a table name and the ssh url of
a LookML project repo and you get a json file that outlines what LookML views refer to said table and what content makes
use of it. Useful for data security audits and probably many other things!

## Requirements

1. Python 3.8+
2. SSH access to a repo containing the LookML for a Looker Project
3. API access to the Looker instance in question

## Installation

1. Clone this repo
2. `pip install .`

## Prior to Running

This tool makes use of the [Looker SDK](https://github.com/looker-open-source/sdk-codegen/tree/master/python) and is expecting to make use of a `looker.ini` file to authenticate to the
API. An example file looks something like this:

```
[looker]
# Base URL for API. Do not include /api/* in the url
base_url=https://self-signed.looker.com:19999
# API 3 client id
client_id=YourClientID
# API 3 client secret
client_secret=YourClientSecret
# Set to false if testing locally against self-signed certs. Otherwise leave True
verify_ssl=True
```

You can have multiple sections in this ini file for multiple Looker instances. See below for more information on
selecting the correct section.

## Usage

`mapper` is the entrypoint to the CLI. Options are:

```
Usage: mapper [OPTIONS]

Options:
  -t, --table TEXT            The table(s) we want to find
  -g, --git-url TEXT          The git url of the LookML repo
  -i, --ini-file TEXT         Path to the ini file to use for sdk
                              authentication

  -l, --looker-instance TEXT  In the ini file, which section to use (i.e.
                              which Looker instance)

  --help                      Show this message and exit.
```

**Example:**

`mapper -t order_items -g git@github.com:company/looker_project.git --ini-file looker.ini --looker-instance looker`

This crawls the LookML project located at `company/looker_project` on Github looking for references to the table
'order_items' It then searches all content on the looker instance specified in the `looker` section of the `looker.ini`
file for any dashboard tile/look that makes use of that table.
