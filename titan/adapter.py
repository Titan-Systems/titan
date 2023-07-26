class Adapter:
    def __init__(self, session):
        self.session = session

    def fetch_account_locator(self):
        with self.session.cursor() as cur:
            locator = cur.execute("SELECT CURRENT_ACCOUNT()").fetchone()[0]
        return locator

    def fetch_region(self):
        with self.session.cursor() as cur:
            region = cur.execute("SELECT CURRENT_REGION()").fetchone()[0]
        return region

    def fetch_database(self, urn):
        database = {}
        with self.session.cursor() as cur:
            for (
                created_on,
                name,
                is_default,
                is_current,
                origin,
                owner,
                comment,
                options,
                retention_time,
                kind,
            ) in cur.execute(f"SHOW DATABASES LIKE '{urn.name}'").fetchall():
                if kind == "STANDARD":
                    database.update(
                        {
                            "name": name,
                            "data_retention_time_in_days": int(retention_time),
                            "comment": comment or None,
                            "transient": options == "TRANSIENT",
                            "owner": owner,
                        }
                    )
            for (
                key,
                value,
                default,
                level,
                description,
                type,
            ) in cur.execute(f"SHOW PARAMETERS IN DATABASE {urn.name}").fetchall():
                if key in ["MAX_DATA_EXTENSION_TIME_IN_DAYS", "DEFAULT_DDL_COLLATION"]:
                    if type == "BOOLEAN":
                        typed_value = value == "true"
                    elif type == "NUMBER":
                        typed_value = int(value)
                    elif type == "STRING":
                        typed_value = str(value) if value else None
                    database[key.lower()] = typed_value
        return database

    def fetch_view(self, urn):
        view = {}

        with self.session.cursor() as cur:
            for (
                created_on,
                name,
                reserved,
                database_name,
                schema_name,
                owner,
                comment,
                text,
                is_secure,
                is_materialized,
                owner_role_type,
                change_tracking,
            ) in cur.execute(f"SHOW VIEWS LIKE '{urn.name}'").fetchall():
                if is_materialized == "true":
                    return {}
                else:
                    view.update(
                        {
                            "name": name,
                            "schema": schema_name,
                            "owner": owner,
                            "comment": comment or None,
                            "as_": text,
                            "secure": is_secure == "true",
                        }
                    )
        return view
