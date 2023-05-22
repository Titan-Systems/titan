import inspect
import importlib
import sys


from titan import __version__, LOGO


import click


@click.group()
def entrypoint():
    """
    >>>>> Titan <<<<<
    """


@click.argument("file")
@entrypoint.command()
def run(file):
    print(LOGO, flush=True)
    print(file)
    module_name = inspect.getmodulename(file)
    print(module_name)
    spec = importlib.util.spec_from_file_location(module_name, file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    print(module)
    app = module.pipeline()
    print(app, flush=True)
    app.create()
    print("DONE")
