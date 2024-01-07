import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--snowflake",
        action="store_true",
        default=False,
        help="Runs tests that require a Snowflake connection",
    )


def pytest_runtest_setup(item):
    if "requires_snowflake" in item.keywords and not item.config.getoption("--snowflake"):
        pytest.skip("need --snowflake option to run this test")
