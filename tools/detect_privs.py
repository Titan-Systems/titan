import os

import snowflake.connector

from titan.data_provider import _show_grants_to_role
from titan.builtins import SYSTEM_ROLES
from titan.privs import SchemaPriv

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
}


def main():
    conn = snowflake.connector.connect(**connection_params)

    grant_owner_pairs = []
    for role in SYSTEM_ROLES:
        grants = _show_grants_to_role(conn, role)
        if grants is None:
            continue
        for grant in grants:
            if grant["grant_option"] == "true" and grant["granted_on"] == "ACCOUNT" and grant["granted_by"] == "":
                grant_owner_pairs.append((role, grant["privilege"]))
    grant_owner_pairs.sort()
    print("GLOBAL_PRIV_DEFAULT_OWNERS = {")
    for role, priv in grant_owner_pairs:
        priv_enum = priv.replace(" ", "_").upper()
        print(f'    AccountPriv.{priv_enum}: "{role}",')
    print("}")

    current_schema_privs = [e.value for e in SchemaPriv]
    print(current_schema_privs)
    print("\n")
    print("class SchemaPriv(ParseableEnum):")
    with conn.cursor() as cur:
        cur.execute("USE ROLE SYSADMIN")
        cur.execute("GRANT ALL ON SCHEMA STATIC_DATABASE.STATIC_SCHEMA TO ROLE CI")
        cur.execute("SHOW GRANTS ON SCHEMA STATIC_DATABASE.STATIC_SCHEMA")
        for grant in cur.fetchall():
            privilege = grant[1]
            grantee_name = grant[5]
            if grantee_name != "CI":
                continue
            if privilege not in current_schema_privs:
                privilege_enum = privilege.replace(" ", "_").upper()
                print(f'    {privilege_enum} = "{privilege}"')


if __name__ == "__main__":
    main()
