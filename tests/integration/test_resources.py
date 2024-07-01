import pytest
from titan import resources as res
from titan.resource_name import ResourceName

from tests.helpers import safe_fetch

pytestmark = pytest.mark.requires_snowflake


def test_user_name_defaults(cursor, suffix):
    user_name = f"user{suffix}_name_defaults"
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == user_name
    cursor.execute(user.create_sql())
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_naked_quoted_name(cursor, suffix):
    user_name = f"~user{suffix}_naked_quoted_name"
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == user_name
    cursor.execute(user.create_sql())
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_quoted_name(cursor, suffix):
    user_name = f'"user{suffix}_quoted_name"'
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == ResourceName(user_name)._name.upper()
    assert user._data.display_name == ResourceName(user_name)._name
    cursor.execute(user.create_sql())
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_name_intentionally_left_blank(cursor, suffix):
    user_name = f"user{suffix}_intentionally_left_blank"
    user = res.User(name=user_name, display_name="", login_name="")
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == ""
    cursor.execute(user.create_sql())
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name
