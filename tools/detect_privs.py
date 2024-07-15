import os

import snowflake.connector

from titan.data_provider import _show_grants_to_role
from titan.builtins import SYSTEM_ROLES

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


if __name__ == "__main__":
    main()
