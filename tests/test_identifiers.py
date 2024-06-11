from titan.resource_name import ResourceName
from titan.identifiers import URN
from titan.parse import parse_URN


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
