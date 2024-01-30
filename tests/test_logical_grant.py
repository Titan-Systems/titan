from titan.logical_grant import And, LogicalGrant, Or


def test_logical_grant_init():
    lg = LogicalGrant("urn", "priv")
    assert lg.urn == "urn"
    assert lg.priv == "priv"


def test_logical_grant_repr():
    lg = LogicalGrant("urn", "priv")
    assert repr(lg) == "LogicalGrant(urn, priv)"


def test_logical_grant_eq():
    lg1 = LogicalGrant("urn", "priv")
    lg2 = LogicalGrant("urn", "priv")
    assert lg1 == lg2


def test_logical_grant_hash():
    lg = LogicalGrant("urn", "priv")
    assert hash(lg) == hash(("urn", "priv"))


def test_logical_grant_or():
    lg1 = LogicalGrant("urn1", "priv1")
    lg2 = LogicalGrant("urn2", "priv2")
    lg3 = LogicalGrant("urn3", "priv3")
    result = lg1 | lg2
    assert isinstance(result, Or)
    assert result.args == (lg1, lg2)
    result = result | lg3
    assert isinstance(result, Or)
    assert result.args == (lg1, lg2, lg3)


def test_logical_grant_and():
    lg1 = LogicalGrant("urn1", "priv1")
    lg2 = LogicalGrant("urn2", "priv2")
    lg3 = LogicalGrant("urn3", "priv3")
    result = lg1 & lg2
    assert isinstance(result, And)
    assert result.args == (lg1, lg2)
    result = result & lg3
    assert isinstance(result, And)
    assert result.args == (lg1, lg2, lg3)
