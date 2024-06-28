import pytest

from titan.parse import format_collection_string, parse_collection_string


# Test cases for parse_collection_string
def test_parse_database_level():
    assert parse_collection_string("SOME_DATABASE.<TABLE>") == {
        "in_name": "SOME_DATABASE",
        "in_type": "database",
        "on_type": "TABLE",
    }


def test_parse_schema_level():
    assert parse_collection_string("SOME_DATABASE.SOME_SCHEMA.<VIEW>") == {
        "in_name": "SOME_DATABASE.SOME_SCHEMA",
        "in_type": "schema",
        "on_type": "VIEW",
    }


def test_parse_invalid_format():
    with pytest.raises(ValueError):
        parse_collection_string("SOME_DATABASE")


def test_parse_incorrect_brackets():
    with pytest.raises(ValueError):
        parse_collection_string("SOME_DATABASE.<TABLE")


# Test cases for format_collection_string
def test_format_database_level():
    collection_dict = {"in_name": "SOME_DATABASE", "in_type": "database", "on_type": "TABLE"}
    assert format_collection_string(collection_dict) == "SOME_DATABASE.<TABLE>"


def test_format_schema_level():
    collection_dict = {"in_name": "SOME_DATABASE.SOME_SCHEMA", "in_type": "schema", "on_type": "VIEW"}
    assert format_collection_string(collection_dict) == "SOME_DATABASE.SOME_SCHEMA.<VIEW>"
