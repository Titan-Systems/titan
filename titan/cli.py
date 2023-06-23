"""
    titan plan
    titan up
    titan crawl
"""

import inspect
import importlib.util
import os
import sys

from . import __version__, LOGO

from .app import App
from .policies.titan_standard import titan_standard

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


@click.argument("path")
@entrypoint.command()
def plan(path: str):
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")

    # Look for titan config file
    cfg = open_config(path) or {}

    app = App(policy=titan_standard)
    for file in os.listdir(path):
        if file.endswith(".sql"):
            print("^" * 80, file)
            app.parse_sql(open(os.path.join(path, file), "r").read())
        elif file.endswith(".py"):
            pass
            # print(file)
            # module_name = inspect.getmodulename(file)
            # if module_name is None:
            #     continue
            # spec = importlib.util.spec_from_file_location(module_name, file)
            # module = importlib.util.module_from_spec(spec)
            # sys.modules[module_name] = module
            # spec.loader.exec_module(module)

    app.build()
    # app.tree()
    # app.run()
    resources = app.resources.sorted()
    print(resources)
    for res in resources:
        # if not res.implicit:
        # print("^" * 120)
        print(repr(res), flush=True)
        # if not res.implicit and not res.stub:
        #     print(res.sql)
    # app.tree()


@entrypoint.command()
def up():
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")


@entrypoint.command()
def crawl():
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")
