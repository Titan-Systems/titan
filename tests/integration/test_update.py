import os

import pytest

from tests.helpers import safe_fetch
from titan import lifecycle
from titan import resources as res
from titan.blueprint import Blueprint

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")


def test_update_schema(cursor, test_db, marked_for_cleanup):
    sch = res.Schema(name="TEST_SCHEMA", database=test_db, max_data_extension_time_in_days=10)
    cursor.execute(sch.create_sql())
    marked_for_cleanup.append(sch)
    result = safe_fetch(cursor, sch.urn)
    assert result["max_data_extension_time_in_days"] == 10
    cursor.execute(lifecycle.update_resource(sch.urn, {"max_data_extension_time_in_days": 9}, res.Schema.props))
    result = safe_fetch(cursor, sch.urn)
    assert result["max_data_extension_time_in_days"] == 9


def test_update_array_props(cursor, test_db, suffix, marked_for_cleanup):
    network_rule_data = {
        "name": f"network_rule_to_update_{suffix}",
        "type": "IPV4",
        "value_list": ["192.168.1.1"],
        "mode": "INGRESS",
        "comment": "Example network rule",
        "database": test_db,
        "schema": "PUBLIC",
        "owner": TEST_ROLE,
    }
    network_rule = res.NetworkRule(**network_rule_data)
    marked_for_cleanup.append(network_rule)
    cursor.execute(network_rule.create_sql())
    result = safe_fetch(cursor, network_rule.urn)
    assert result["value_list"] == ["192.168.1.1"]

    network_rule_data["value_list"] = ["192.168.1.1", "192.168.1.2"]
    network_rule = res.NetworkRule(**network_rule_data)
    bp = Blueprint()
    bp.add(network_rule)
    plan = bp.plan(cursor.connection)
    assert len(plan) == 1
    bp.apply(cursor.connection, plan)
    result = safe_fetch(cursor, network_rule.urn)
    assert result["value_list"] == ["192.168.1.1", "192.168.1.2"]
