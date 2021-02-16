from looker_data_mapper import table_mapper
from lookml import View, File


# create mock lookml views
mock_view_tacocat = View("mock_view_tacocat")
mock_view_tacocat + "sql_table_name: tacocat;;"

mock_view_dt_tacocat = View("mock_view_dt_tacocat")
mock_view_dt_tacocat + "derived_table: {sql: select id from tacocat;;}"

mock_view_dt_ref_tacocat = View("mock_view_dt_ref_tacocat")
mock_view_dt_ref_tacocat + "derived_table: {sql: select id from ${mock_view_tacocat.SQL_TABLE_NAME};;}"

mock_view_burritocat = View("mock_view_burritocat")
mock_view_burritocat + "sql_table_name: burritocat;;"

mock_view_dt_burritocat = View("mock_view_dt_burritocat")
mock_view_dt_burritocat + "derived_table: {sql: select id from burritocat;;}"

mock_view_dt_ref_burritocat = View("mock_view_dt_ref_burritocat")
mock_view_dt_ref_burritocat + "derived_table: {sql: select id from ${mock_view_burritocat.SQL_TABLE_NAME};;}"

# create mock lookml files
mock_taco_file = File(mock_view_tacocat)

mock_taco_file_dt = File(mock_view_dt_tacocat)
mock_taco_file_dt + mock_view_dt_ref_tacocat

mock_burrito_file = File(mock_view_burritocat)


# create a mock lookml project
class MockProject():
    mock_files = [mock_taco_file, mock_taco_file_dt, mock_burrito_file]

    def files(self):
        return self.mock_files


# create mock dashboard elements
class MockQuery():
    def __init__(self, fields):
        self.fields = fields


class MockLook():
    def __init__(self, title, query):
        self.title = title
        self.query = query


class MockDashboard():
    def __init__(self, dash_id, title):
        self.id = dash_id
        self.title = title


class MockDashboardElement():
    def __init__(self, dash_elem_id, title=None, query=None, look=None):
        self.id = dash_elem_id
        self.title = title
        self.query = query
        self.look = look
        self.type = "vis"


class MockSDK():
    def all_dashboards():
        pass

    def dashboard_dashboard_elements():
        pass


def test_check_sql_table_name_found():
    resp = table_mapper.check_sql_table_name(mock_view_tacocat, "tacocat")
    assert resp


def test_check_sql_table_name_not_found():
    resp = table_mapper.check_sql_table_name(mock_view_tacocat, "burritocat")
    assert resp is False


def test_check_sql_table_name_failure():
    resp = table_mapper.check_sql_table_name(mock_view_dt_tacocat, "tacocat")
    assert resp is False


def test_check_derived_table_found():
    resp = table_mapper.check_derived_table(mock_view_dt_tacocat, "tacocat")
    assert resp


def test_check_derived_table_not_found():
    resp = table_mapper.check_derived_table(mock_view_dt_tacocat, "burritocat")
    assert resp is False


def test_check_derived_table_failure():
    resp = table_mapper.check_derived_table(mock_view_tacocat, "tacocat")
    assert resp is False


def test_check_derived_table_ref_found():
    resp = table_mapper.check_derived_table_ref(mock_view_dt_ref_tacocat)
    assert resp == {"mock_view_dt_ref_tacocat": ["mock_view_tacocat"]}


def test_check_derived_table_ref_not_found():
    resp = table_mapper.check_derived_table_ref(mock_view_dt_tacocat)
    assert resp is False


def test_check_derived_table_ref_failure():
    resp = table_mapper.check_derived_table_ref(mock_view_tacocat)
    assert resp is False


def test_crawl_dt_ref_dict():
    matched_views = ["foo", "bar"]
    dt_ref = {"taco": ["foo", "baz"], "cat": ["baz", "bosh"]}

    table_mapper.crawl_dt_ref_dict(dt_ref, matched_views)

    assert matched_views == ["foo", "bar", "taco"]


def test_crawl_dt_ref_dict_recursion():
    matched_views = ["foo", "bar"]
    dt_ref = {"taco": ["foo", "baz"], "cat": ["baz", "bosh"], "burrito": ["taco"], "dog": ["burrito"]}

    table_mapper.crawl_dt_ref_dict(dt_ref, matched_views)

    assert matched_views == ["foo", "bar", "taco", "burrito", "dog"]


def test_crawl_dt_ref_dict_recursion_order():
    matched_views = ["foo", "bar"]
    dt_ref = {"taco": ["foo", "baz"], "cat": ["baz", "bosh"], "burrito": ["dog"], "dog": ["taco"]}

    table_mapper.crawl_dt_ref_dict(dt_ref, matched_views)

    assert matched_views == ["foo", "bar", "taco", "dog", "burrito"]


def test_fetch_views():
    project = MockProject()
    table_name = "tacocat"

    view_list = table_mapper.fetch_views(table_name, project)
    assert view_list == ["mock_view_tacocat", "mock_view_dt_tacocat", "mock_view_dt_ref_tacocat"]


def test_get_dashboards(mocker):
    sdk = MockSDK()
    mocker.patch.object(sdk, "all_dashboards")
    mocker.patch.object(sdk, "dashboard_dashboard_elements")

    query = MockQuery(["tacocat.foo", "burritocat.bar"])
    look_query = MockQuery(["tacocat.baz"])

    look = MockLook("taco_look", look_query)

    dash = MockDashboard("1", "foobar")
    elem = MockDashboardElement("1", title="query element", query=query)
    elem_look = MockDashboardElement("2", look=look)

    sdk.all_dashboards.return_value = [dash]
    sdk.dashboard_dashboard_elements.return_value = [elem, elem_look]

    dashboard_list = table_mapper.get_dashboards(sdk)
    expected_element_query = {"id": "1", "title": "query element", "fields": ["tacocat.foo", "burritocat.bar"]}
    expected_element_look = {"id": "2", "title": "taco_look", "fields": ["tacocat.baz"]}
    expected_result = [{"id": "1", "title": "foobar", "elements": [expected_element_query, expected_element_look]}]

    assert dashboard_list == expected_result


def test_get_table_refs(mocker):

    mocker.patch("looker_data_mapper.table_mapper.fetch_views")
    views = ["mock_view_tacocat", "mock_view_dt_tacocat", "mock_view_dt_ref_tacocat"]
    table_mapper.fetch_views.return_value = views

    dash_element_query = {
        "id": "1",
        "title": "query element",
        "fields": ["mock_view_tacocat.foo", "mock_view_burritocat.bar"]
    }

    dash_element_look = {"id": "2", "title": "taco_look", "fields": ["mock_view_dt_tacocat.baz"]}
    dash_result = [{"id": "1", "title": "foobar", "elements": [dash_element_query, dash_element_look]}]

    mocker.patch("builtins.open", mocker.mock_open(read_data="foobar"))
    mocker.patch("json.dump")

    resp = table_mapper.get_table_refs("tacocat", "foo", dash_result)

    expected_elems = ["1_query element", "2_taco_look"]
    expected = {
        "lookml_views": ["mock_view_tacocat", "mock_view_dt_tacocat", "mock_view_dt_ref_tacocat"],
        "content": [{"dashboard_id": "1", "dashboard_title": "foobar", "dashboard_tiles": expected_elems}]
    }
    assert resp == expected
