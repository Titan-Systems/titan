import pytest

from titan.resources import Account, Alert, JavascriptUDF, View

resources = [
    {
        "test": "account",
        "resource_cls": Account,
        "data": {
            "name": "SOMEACCOUNT",
        },
    },
    {
        "test": "alert",
        "resource_cls": Alert,
        "data": {
            "name": "ALERT",
            "owner": "SYSADMIN",
            "warehouse": "wh",
            "schedule": "1 minute",
            "condition": "SELECT 1",
            "then": "INSERT INTO foo VALUES(1)",
        },
    },
    {
        "test": "javascript_udf",
        "resource_cls": JavascriptUDF,
        "data": {
            "name": "NOOP",
            "owner": "SYSADMIN",
            "args": [],
            "secure": False,
            "returns": "FLOAT",
            "language": "JAVASCRIPT",
            "volatility": "VOLATILE",
            "as_": "return 42;",
        },
    },
    {
        "test": "view",
        "resource_cls": View,
        "data": {"name": "MY_VIEW", "owner": "SYSADMIN", "volatile": True, "as_": "SELECT * FROM tbl"},
    },
]


@pytest.fixture(
    params=resources,
    ids=[f"test_{config['test']}" for config in resources],
    scope="function",
)
def resource(request):
    resource = request.param
    yield resource


def dump(resource):
    return resource.model_dump(mode="json", by_alias=True, exclude_none=True)


def test_data_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    assert dump(instance) == resource["data"]


def test_sql_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    sql = instance.create_sql()
    assert resource["resource_cls"].from_sql(sql) == instance
