"""
    titan plan
    titan up
    titan crawl
"""

import os
import time

from . import __version__, LOGO

from .policies.titan_standard import titan_standard
from .resource import Resource
from .parse import _split_statements

import click
import yaml


def open_config(path: str):
    file = os.path.join(path, "titan.yaml")
    with open(file, "r") as f:
        return yaml.safe_load(f)


@click.group()
def entrypoint():
    """
    >>>>> Titan <<<<<
    """


# @click.argument("path")
# @entrypoint.command()
# def plan(path: str):
#     print(LOGO, flush=True)
#     print(f"      Titan v{__version__}\n")

#     app = App(policy=titan_standard)

#     if os.path.isfile(path):
#         app.parse_sql(open(path, "r").read())
#         return

#     # Look for titan config file
#     cfg = open_config(path) or {}

#     for file in os.listdir(path):
#         if file.endswith(".sql"):
#             print("^" * 80, file)
#             app.parse_sql(open(os.path.join(path, file), "r").read())
#         elif file.endswith(".py"):
#             pass
#             # print(file)
#             # module_name = inspect.getmodulename(file)
#             # if module_name is None:
#             #     continue
#             # spec = importlib.util.spec_from_file_location(module_name, file)
#             # module = importlib.util.module_from_spec(spec)
#             # sys.modules[module_name] = module
#             # spec.loader.exec_module(module)

#     app.build()
#     # app.tree()
#     # app.run()
#     resources = app.resources.sorted()
#     print(resources)
#     for res in resources:
#         # if not res.implicit:
#         print("^" * 120, flush=True)
#         print(repr(res), flush=True)
#         if res.stub:
#             print("--stub--", flush=True)
#         elif res.implicit:
#             pass
#         else:
#             print(res.sql, flush=True)
#         # if not res.implicit and not res.stub:
#         #     print(res.sql)
#     # app.tree()


@entrypoint.command()
def up():
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")


@entrypoint.command()
def crawl():
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")


@click.argument("path")
@entrypoint.command()
def test(path: str):
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")

    now = time.time()

    for file in sorted(os.listdir(path)):
        if file.endswith(".sql"):
            print("^" * 80, file)
            start = time.time()
            sql_blob = open(os.path.join(path, file), "r").read()
            # try:
            for raw in _split_statements(sql_blob):
                sql = raw.strip()
                # print(">>>", sql, "<<<")
                if sql:
                    command = sql.split()[0].lower()
                    if command != "create":
                        # print(f"Command {command} not supported")
                        continue
                    res = Resource.from_sql(sql)
                    if res:
                        print(
                            "✅",
                            f"<{res.__class__.__name__} {res.name}>",
                            # "=>",
                            # res.model_dump(exclude_none=True, exclude_defaults=True),
                        )
            print(f"Elapsed: {time.time() - start:.2f}s")
    print(f">>>> Elapsed: {time.time() - now:.2f}s")
