import pytest

from titan import __version__


@pytest.mark.requires_snowflake
def test_install(suffix, cursor):
    install = open("scripts/install", "r").read()
    install = install.replace(__version__, f"{__version__}-dev")
    cursor.execute("USE ROLE SYSADMIN")
    cursor.execute(f"CREATE DATABASE TITAN_SPI_TEST_{suffix}")
    cursor.execute(f"CREATE STAGE TITAN_SPI_TEST_{suffix}.PUBLIC.TITAN_AWS URL = 's3://titan-snowflake/';")
    cursor.execute(install)
