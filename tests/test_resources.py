# tests/test_entities.py

import pytest
from titan import Warehouse, User

# A list of test cases for the Warehouse class.
# Each test case is a tuple containing the SQL to parse and the expected result.
WAREHOUSE_TESTS = [
    (
        """CREATE WAREHOUSE IF NOT EXISTS my_warehouse
            MAX_CLUSTER_COUNT = 10
            MIN_CLUSTER_COUNT = 5
            WITH TAG (tag1 = 'value1', tag2 = 'value2')""",
        {"max_cluster_count": 10, "min_cluster_count": 5, "tags": {"tag1": "value1", "tag2": "value2"}},
    ),
    # Add more test cases here...
]


@pytest.mark.parametrize("sql,expected", WAREHOUSE_TESTS)
def test_warehouse(sql, expected):
    result = Warehouse.parse_props(sql)
    assert result == expected


# Similar test cases for the User class
USER_TESTS = [
    (
        """CREATE USER IF NOT EXISTS jack
            LOGIN_NAME = 'jack'
            FIRST_NAME = SNOWPARK
            LAST_NAME = 'HOL'
            EMAIL = 'jack@hol.snowpark'
            MUST_CHANGE_PASSWORD = FALSE""",
        {
            "login_name": "jack",
            "first_name": "SNOWPARK",
            "last_name": "HOL",
            "email": "jack@hol.snowpark",
            "must_change_password": False,
        },
    ),
    # Add more test cases here...
]


@pytest.mark.parametrize("sql,expected", USER_TESTS)
def test_user(sql, expected):
    result = User.parse_props(sql)
    assert result == expected
