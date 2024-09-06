import titan.resources as res


def test_resource_pointer_rendering():
    db = res.Database(name="DB")
    schema = res.Schema(name="SCH")
    network_rule = res.NetworkRule(
        name="TITAN_TEST_NETWORK_RULE",
        type="IPV4",
        value_list=["85.83.225.229"],
        mode="INGRESS",
        database=db,
        schema=schema,
    )

    network_policy = res.NetworkPolicy(
        name="TITAN_TEST_NETWORK_POLICY",
        allowed_network_rule_list=[network_rule],
        blocked_network_rule_list=None,
        allowed_ip_list=None,
        blocked_ip_list=None,
        database=db,
        schema=schema,
    )
    rendered = network_policy.create_sql()
    assert (
        rendered
        == "CREATE NETWORK POLICY TITAN_TEST_NETWORK_POLICY ALLOWED_NETWORK_RULE_LIST = (DB.SCH.TITAN_TEST_NETWORK_RULE)"
    )
