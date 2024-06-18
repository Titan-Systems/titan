import pyparsing as pp

from titan import resources as res
from titan.resource_name import ResourceName
from titan.parse import FullyQualifiedIdentifier, parse_URN

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
    rn2 = ResourceName("test")
    assert rn1 == rn2

    rn1 = ResourceName('"test"')
    rn2 = ResourceName('"test"')
    assert rn1 == rn2

    rn1 = ResourceName("test")
    rn2 = ResourceName('"test"')
    assert rn1 != rn2

    rn1 = ResourceName("test")
    rn2 = ResourceName("foo")
    assert rn1 != rn2


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
