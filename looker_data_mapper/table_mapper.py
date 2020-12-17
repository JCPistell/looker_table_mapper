import looker_sdk
import json
import lookml
import re
import concurrent.futures
from pathlib import Path
from itertools import repeat


def check_sql_table_name(view, table_name):
    """Accepts a view object and a table name. Returns true if the view contains the table name."""
    try:
        if view.sql_table_name.value == table_name:
            return True
        else:
            return False
    except (KeyError, SyntaxError):
        return False


def check_derived_table(view, table_name):
    """Accepts a view object and a table name. Returns true if the view uses a derived table and that
    derived table contains the table name in the from or join clauses.
    """

    # Check for table name in from or join clause
    pattern_text = f"from {table_name}|join {table_name}"
    pattern = re.compile(pattern_text, re.IGNORECASE)

    try:
        dt = view.derived_table
        match = re.search(pattern, str(dt.value))
        if match:
            return True
        else:
            return False
    except (KeyError, SyntaxError):
        return False


def check_derived_table_ref(view):
    """Determines if there is a view reference in a derived table. If so, it returns true. The view can then
    be appended to a list of views to check once the primary loop has completed.
    """

    pattern_text = r"\${.*\.SQL_TABLE_NAME}"
    pattern = re.compile(pattern_text)

    try:
        dt = view.derived_table
        matches = list(set(re.findall(pattern, str(dt.value))))
        if matches:
            # extract the view names
            parsed_matches = [i.split(".")[0].replace("${", "") for i in matches]

            # return the dict of the view with it's referenced view names
            return {view.name: parsed_matches}
        else:
            return False

    except (KeyError, SyntaxError):
        return False


def crawl_dt_ref_dict(dt_ref_dict, matched_view_list):
    """This one is confusing. This handles any deferred derived table views that referenced other
    derived table views with a SQL_TABLE_NAME parameter. We first check to see if any referenced
    views (the value of each dict in the dt_ref_list) are included in the list... if so we add
    those to a new list to defer them further. We then check each entry in the list of referenced
    views and if they appear already in the matched_view_list then we append the key view name to
    that list and move on. In the end, if there are any entries in the deferred list we recursively
    call this function again (and again etc) until there are no more entries to check.
    """

    dt_ref_view_names = list(dt_ref_dict.keys())

    defer_dict = {}
    for key, value in dt_ref_dict.items():
        # check if any view values are in this dict
        if any(item in value for item in dt_ref_view_names):
            # add to defer dict and skip to next iteration
            defer_dict.update({key: value})
            continue

        # now lets check to see if any values are in the matched views
        if any(item in value for item in matched_view_list):
            matched_view_list.append(key)

    if defer_dict:
        # recursive call
        print("recursing!")
        print(defer_dict)
        crawl_dt_ref_dict(defer_dict, matched_view_list)


def fetch_views(table_name, proj):
    """Accepts a table name and a pylookml project and returns a list of all LookML views
    that reference the table. Makes use of deferred processing, reverse indexing, and
    recursion to handle nested view references in derived tables.
    """

    view_list = []
    defer_dict = {}
    for file in proj.files():
        for view in file.views:
            if check_sql_table_name(view, table_name):
                view_list.append(view.name)
            elif check_derived_table(view, table_name):
                view_list.append(view.name)
            else:
                defer_view = check_derived_table_ref(view)
                if defer_view:
                    defer_dict.update(defer_view)

    # Main loop complete - now we recurse through the deferred dict

    crawl_dt_ref_dict(defer_dict, view_list)

    return view_list


def get_dashboards(sdk):
    """Accepts an instance reference and pulls all relevant dashboard info via the Looker API.
    This info includes dashboard element info such as title and field data, which we can then
    use to determine which dashboards and dashboard elements reference a table of interest.
    Returns a list that can be used to compare to views of interest.
    """

    # initialize an empty dict container that will hold the final returned objects
    dash_element_fields = []

    dashboards = sdk.all_dashboards(fields="title, id")

    for dash in dashboards:
        dash_entry = {"id": dash.id, "title": dash.title, "elements": []}
        elements = sdk.dashboard_dashboard_elements(dash.id, fields="id, title, query, look")

        for elem in elements:
            # element field info will either be in a query or a look. we try both
            try:
                fields = elem.query.fields
            except AttributeError:
                fields = elem.look.query.fields

            title = elem.title or elem.look.title

            elem_entry = {"id": elem.id, "title": title, "fields": fields}
            dash_entry["elements"].append(elem_entry)

        dash_element_fields.append(dash_entry)

    return dash_element_fields


def get_table_refs(table, project, dashboards_list):
    """Accepts a table name and a full dashboard dict. Writes a JSON file with the parsed
    results and returns the associated dict.
    """

    views = fetch_views(table, project)

    if not views:
        print("No LookML Views found! We're done here!")
        return 0

    final_dict = {"lookml_views": views, "content": []}
    for dashboard in dashboards_list:
        elem_list = []
        for elem in dashboard["elements"]:
            # get a distinct list of just the views
            field_views = list(set([i.split(".")[0] for i in elem["fields"]]))

            # compare element query/look views to table views
            if any(item in views for item in field_views):
                elem_list.append(f"{elem['id']}_{elem['title']}")

        if elem_list:
            final_dict["content"].append({
                "dashboard_id": dashboard["id"],
                "dashboard_title": dashboard["title"],
                "dashboard_tiles": elem_list
            })

    with open(f"{table}.json", "w") as f:
        json.dump(final_dict, f)

    return final_dict


def main(**kwargs):
    git_url = kwargs["git_url"]
    tables = kwargs["table"]
    cwd = Path.cwd()
    looker_instance = kwargs.get("looker_instance")
    ini_file = kwargs.get("ini_file")

    if ini_file:
        parsed_ini_file = cwd.joinpath(ini_file)
    else:
        parsed_ini_file = None

    project = lookml.Project(git_url=git_url)

    sdk = looker_sdk.init31(config_file=parsed_ini_file, section=looker_instance)
    dashboards = get_dashboards(sdk)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        pool.map(get_table_refs, tables, repeat(project), repeat(dashboards))
