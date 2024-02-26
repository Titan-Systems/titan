from titan.resources import ResourceName


def test_resource_name():
    rn = ResourceName("test")
    assert str(rn) == "test"

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
