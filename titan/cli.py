import inspect
import importlib.util
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
def run(file: str):
    print(LOGO, flush=True)
    print(f"      Titan v{__version__}\n")

    # Load the module that CLI args point at
    module_name = inspect.getmodulename(file)
    if module_name is None:
        return
    spec = importlib.util.spec_from_file_location(module_name, file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # module should have something called `app` in it
    # TODO: expand support
    app = module.app
    app.build()
    app.tree()
    app.run()
