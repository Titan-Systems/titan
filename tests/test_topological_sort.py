from titan.blueprint import topological_sort

resource_set = {
    'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
    "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    "urn:::role/private_equity_associate",
    'urn:::user/"c.black@arrakisinvestments.com"',
    'urn:::role_grant/private_equity_associate?user="d.gray@arrakisinvestments.com"',
    'urn:::user/"d.gray@arrakisinvestments.com"',
    "urn:::role/private_equity_analyst",
    "urn:::role_grant/private_equity_associate?role=private_equity_analyst",
    "urn:::role_grant/private_equity_associate?role=private_equity_admin",
    "urn:::role/private_equity_admin",
    "urn:::role_grant/private_equity_admin?role=private_equity_associate",
}


refs = [
    (
        'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
        "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    ),
    (
        'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
        "urn:::role/private_equity_associate",
    ),
    (
        'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
        'urn:::user/"c.black@arrakisinvestments.com"',
    ),
    (
        'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
        "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    ),
    (
        'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
        "urn:::role/private_equity_associate",
    ),
    (
        'urn:::role_grant/private_equity_associate?user="c.black@arrakisinvestments.com"',
        'urn:::user/"c.black@arrakisinvestments.com"',
    ),
    (
        'urn:::role_grant/private_equity_associate?user="d.gray@arrakisinvestments.com"',
        "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    ),
    (
        'urn:::role_grant/private_equity_associate?user="d.gray@arrakisinvestments.com"',
        "urn:::role/private_equity_associate",
    ),
    (
        'urn:::role_grant/private_equity_associate?user="d.gray@arrakisinvestments.com"',
        'urn:::user/"d.gray@arrakisinvestments.com"',
    ),
    (
        "urn:::role_grant/private_equity_associate?role=private_equity_analyst",
        "urn:::role/private_equity_analyst",
    ),
    (
        "urn:::role_grant/private_equity_associate?role=private_equity_analyst",
        "urn:::role/private_equity_associate",
    ),
    (
        "urn:::role_grant/private_equity_associate?role=private_equity_analyst",
        "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    ),
    (
        "urn:::role_grant/private_equity_associate?role=private_equity_admin",
        "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    ),
    (
        "urn:::role_grant/private_equity_associate?role=private_equity_admin",
        "urn:::role/private_equity_associate",
    ),
    (
        "urn:::role_grant/private_equity_associate?role=private_equity_admin",
        "urn:::role/private_equity_admin",
    ),
    (
        "urn:::role_grant/private_equity_admin?role=private_equity_associate",
        "urn:::account/TITAN_TEST_AWS_ENTERPRISE",
    ),
    (
        "urn:::role_grant/private_equity_admin?role=private_equity_associate",
        "urn:::role/private_equity_associate",
    ),
    (
        "urn:::role_grant/private_equity_admin?role=private_equity_associate",
        "urn:::role/private_equity_admin",
    ),
    ("urn:::role/private_equity_associate", "urn:::account/TITAN_TEST_AWS_ENTERPRISE"),
    ('urn:::user/"c.black@arrakisinvestments.com"', "urn:::account/TITAN_TEST_AWS_ENTERPRISE"),
]


def test_topological_sort():
    sorted_resources = topological_sort(resource_set, set(refs))
    assert len(sorted_resources) == len(resource_set)
