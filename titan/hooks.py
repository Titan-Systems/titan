import snowflake.snowpark  # type: ignore


def on_file_added_factory(_stage, _hook):
    _hook.__module__ = "__main__"

    def sproc(session: snowflake.snowpark.session.Session) -> dict:
        import snowflake.snowpark
        import sys
        import os

        hook = _hook

        def list_new_files(stage):
            # TODO: create a highwatermark system or find a more elegant way to do this
            res = session.sql(
                f"""
                SELECT
                    relative_path
                FROM DIRECTORY(@{stage})
                WHERE
                    last_modified >= '2023-01-01'
                """
            ).collect()
            return [row[0] for row in res]

        stage = _stage
        new_files = list_new_files(stage)

        hook.__module__ = "__main__"
        session.add_packages("snowflake-snowpark-python")
        sp = session.sproc.register(
            hook,
            imports=[f"@{stage}/{file}" for file in new_files],
            is_permanent=False,
        )
        # TODO: Make this async and parallel
        sp_res = sp(new_files)
        return {"result": sp_res}

    return sproc


# def on_file_added_factory(_stage, _hook):
#     def sproc() -> dict:
#         import snowflake.snowpark
#         import sys
#         import os

#         hook = _hook

#         def list_new_files(stage):
#             # TODO: create a highwatermark system or find a more elegant way to do this
#             res = session.sql(
#                 f"""
#                 SELECT
#                     relative_path
#                 FROM DIRECTORY(@{stage})
#                 WHERE
#                     last_modified >= '2023-01-01'
#                 """
#             ).collect()
#             return [row[0] for row in res]

#         def main(session: snowflake.snowpark.session.Session) -> dict:
#             stage = _stage
#             new_files = list_new_files(stage)

#             hook.__module__ = "__main__"
#             session.add_packages("snowflake-snowpark-python")
#             sp = session.sproc.register(
#                 _hook,
#                 imports=[f"@{stage}/{file}" for file in new_files],
#                 is_permanent=False,
#             )
#             sp_res = sp(files)
#             return {"result": sp_res}

#     return sproc
