import pytest

from titan.blueprint import Blueprint
from titan.resources import Database, Schema, Role, Warehouse


def test_container_service():
    "Test a container service in Titan end-to-end"

    bp = Blueprint()

    db = Database(name="container_test_db")
    sch = Schema(name="container_test_schema")
    db.add(sch)

    admin_role = Role(name="container_test_admin_role")
    wh = Warehouse(name="container_test_wh", auto_suspend=60, auto_resume=True)

    compute_pool = ComputePool(
        name="titan_app_compute_pool_test", min_nodes=1, max_nodes=1, instance_family="CPU_X64_XS"
    )

    security_integration = SecurityIntegration(
        name="Application Authentication Test",
        integration_type="oauth",
        oauth_client="snowservices_ingress",
        enabled=True,
    )

    bp.add(compute_pool, security_integration)

    # Grant permissions
    bp.grant_all_on_database("titan_db_test", "container_test_admin_role")
    bp.grant_all_on_schema("titan_db_test.titan_app_test", "container_test_admin_role")
    bp.grant_select_on_all_tables("titan_db_test.titan_app_test", "container_test_admin_role")
    bp.grant_select_on_future_tables("titan_db_test.titan_app_test", "container_test_admin_role")

    bp.grant_all_on_warehouse("container_test_wh", "container_test_admin_role")

    bp.grant_usage_on_compute_pool("titan_app_compute_pool_test", "container_test_admin_role")
    bp.grant_monitor_on_compute_pool("titan_app_compute_pool_test", "container_test_admin_role")

    bp.grant_ownership_on_integration("Application Authentication Test", "container_test_admin_role")

    bp.grant_bind_service_endpoint_on_account("container_test_admin_role")

    bp.add(db, admin_role, wh)
