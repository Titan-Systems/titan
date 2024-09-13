import pytest

import pyparsing as pp

from titan import resources as res
from titan.identifiers import parse_URN
from titan.parse import FullyQualifiedIdentifier
from titan.resource_name import ResourceName

IDENTIFIER_TEST_CASES = [
    ("tbl", ["tbl"]),
    ("schema.tbl", ["schema", "tbl"]),
    ("DB.SCHEMA", ["DB", "SCHEMA"]),
    ("db.schema.tbl", ["db", "schema", "tbl"]),
    ('"tbl"', ['"tbl"']),
    ('"schema"."tbl"', ['"schema"', '"tbl"']),
    ('"db"."schema"."tbl"', ['"db"', '"schema"', '"tbl"']),
    ('db."schema"."tbl"', ["db", '"schema"', '"tbl"']),
    ('"db".schema."tbl"', ['"db"', "schema", '"tbl"']),
    ('"db"."schema".tbl', ['"db"', '"schema"', "tbl"]),
    ('db."this.is:schema".tbl', ["db", '"this.is:schema"', "tbl"]),
    ("TITAN_CLASS_TEST.PUBLIC.BASIC_MODEL.USER", ["TITAN_CLASS_TEST", "PUBLIC", "BASIC_MODEL", "USER"]),
]


def test_parse_identifier():
    for test_case, result in IDENTIFIER_TEST_CASES:
        assert FullyQualifiedIdentifier.parse_string(test_case).as_list() == result


def test_parse_identifier_wrapped():
    for test_case, result in IDENTIFIER_TEST_CASES:
        assert pp.MatchFirst(FullyQualifiedIdentifier).parse_string(test_case).as_list() == result


def test_urn():
    urn = parse_URN("urn::ABCD123:storage_integration/GCS_INT")
    assert str(urn) == "urn::ABCD123:storage_integration/GCS_INT"

    assert urn == parse_URN(str(urn))
    assert urn == parse_URN("urn::ABCD123:storage_integration/gcs_int")
    assert hash(urn) == hash(parse_URN("urn::ABCD123:storage_integration/gcs_int"))


def test_resource_name():
    rn = ResourceName("test")
    assert str(rn) == "TEST"

    rn = ResourceName('"test"')
    assert str(rn) == '"test"'
    assert rn._quoted

    rn = ResourceName("foo@bar.com")
    assert str(rn) == '"foo@bar.com"'
    assert rn._quoted


def test_resource_name_equality():
    rn1 = ResourceName("test")
    rn2 = ResourceName("TEST")
    assert rn1 == rn2

    rn1 = ResourceName('"test"')
    rn2 = ResourceName('"test"')
    assert rn1 == rn2

    rn1 = ResourceName("test")
    rn2 = ResourceName('"TEST"')
    assert rn1 == rn2

    rn1 = ResourceName("test")
    rn2 = ResourceName('"test"')
    assert rn1 != rn2

    rn1 = ResourceName("test")
    rn2 = ResourceName("foo")
    assert rn1 != rn2


def test_resource_name_string_comparison():
    assert "FOO" in [ResourceName("foo"), ResourceName("bar")]
    assert ResourceName("FOO") in ["foo", "bar"]
    assert "FOO" == ResourceName("FOO")
    assert "FOO" == ResourceName("foo")
    assert "foo" == ResourceName("FOO")
    assert "foo" == ResourceName("foo")
    assert ResourceName("FOO") == "FOO"
    assert ResourceName("foo") == "FOO"
    assert ResourceName("FOO") == "foo"
    assert ResourceName("foo") == "foo"

    # Quoted identifiers
    assert ResourceName('"FOO"') == ResourceName("FOO")
    assert ResourceName('"FOO"') == ResourceName("foo")
    assert ResourceName('"FOO"') == "foo"
    assert ResourceName('"FOO"') == "FOO"


def test_parse_fully_qualified_schema():
    sch = res.Schema(name="DB.SCHEMA")
    assert sch.name == "SCHEMA"
    assert sch.container.name == "DB"

    sch = res.Schema.from_sql("CREATE SCHEMA IF NOT EXISTS DB.SCHEMA")
    assert sch.name == "SCHEMA"
    assert sch.container.name == "DB"

    # tbl = res.Table(name="DB.SCHEMA.TABLE", columns=[res.Column(name="ID", data_type="INT")])
    # assert tbl.name == "TABLE"
    # assert tbl.container.name == "SCHEMA"
    # assert tbl.container.container.name == "DB"


def test_resource_name_type_checking():
    with pytest.raises(RuntimeError):
        ResourceName(111)
