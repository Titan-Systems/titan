import json

import click
import yaml

from titan.blueprint import dump_plan
from titan.operations.blueprint import blueprint_apply, blueprint_plan
from titan.operations.export import export_resources
from titan.operations.connector import connect, get_env_vars

from .identifiers import resource_type_for_label


class JsonParamType(click.ParamType):
    name = "json"

    def convert(self, value, param, ctx):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            self.fail(f"'{value}' is not a valid JSON string", param, ctx)


def load_config(config_file):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config


def load_plan(plan_file):
    with open(plan_file, "r") as f:
        plan = json.load(f)
    return plan


@click.group()
def titan_cli():
    """titan core helps you manage your Snowflake environment."""
    pass


@titan_cli.command("plan", no_args_is_help=True)
@click.option("--config", "config_file", type=str, help="Path to configuration YAML file", metavar="<filename>")
@click.option("--json", "json_output", is_flag=True, help="Output plan in machine-readable JSON format")
@click.option("--out", "output_file", type=str, help="Write plan to a file", metavar="<filename>")
@click.option("--vars", type=JsonParamType(), help="Vars to pass to the blueprint")
@click.option(
    "--mode",
    "run_mode",
    type=click.Choice(["CREATE-OR-UPDATE", "SYNC"]),
    metavar="<run_mode>",
    show_default=True,
    help="Run mode",
)
def plan(config_file, json_output, output_file, vars: dict, run_mode):
    """Generate an execution plan based on your configuration"""
    yaml_config = load_config(config_file)
    cli_config = {}
    if vars:
        cli_config["vars"] = vars
    if run_mode:
        cli_config["run_mode"] = run_mode
    plan_obj = blueprint_plan(yaml_config, cli_config)
    output = None
    if json_output:
        output = dump_plan(plan_obj, format="json")
    else:
        output = dump_plan(plan_obj, format="text")
    if output_file:
        with open(output_file, "w") as f:
            f.write(output)
    else:
        print(output)


@titan_cli.command("apply", no_args_is_help=True)
@click.option("--config", "config_file", type=str, help="Path to configuration YAML file", metavar="<filename>")
@click.option("--plan", "plan_file", type=str, help="Path to plan JSON file", metavar="<filename>")
@click.option("--vars", type=JsonParamType(), help="Vars to pass to the blueprint")
@click.option(
    "--mode",
    "run_mode",
    type=click.Choice(["CREATE-OR-UPDATE", "SYNC"]),
    metavar="<run_mode>",
    show_default=True,
    help="Run mode",
)
@click.option("--dry-run", is_flag=True, help="Perform a dry run without applying changes")
def apply(config_file, plan_file, vars, run_mode, dry_run):
    """Apply an execution plan to a Snowflake account"""
    if config_file and plan_file:
        raise click.UsageError("Cannot specify both --config and --plan.")
    if not config_file and not plan_file:
        raise click.UsageError("Either --config or --plan must be specified.")
    if config_file:
        yaml_config = load_config(config_file)
        cli_config = {}
        if vars:
            cli_config["vars"] = vars
        if run_mode:
            cli_config["run_mode"] = run_mode
        if dry_run:
            cli_config["dry_run"] = dry_run
        blueprint_apply(yaml_config, cli_config)
    else:
        plan_obj = load_plan(plan_file)
        blueprint_apply(plan_obj, run_mode, dry_run)


def _parse_resources(resource_labels_str):
    if resource_labels_str is None:
        return None
    return [resource_type_for_label(resource_label) for resource_label in resource_labels_str.split(",")]


@titan_cli.command("export", context_settings={"show_default": True}, no_args_is_help=True)
@click.option("--resource", type=str, help="The resource types to export", metavar="<resource_types>")
@click.option("--all", "export_all", is_flag=True, help="Export all resources")
@click.option("--exclude", "exclude", type=str, help="Exclude resources, used with --all", metavar="<resource_types>")
@click.option("--out", type=str, help="Write exported config to a file", metavar="<filename>")
@click.option("--format", type=click.Choice(["json", "yml"]), default="yml", help="Output format")
def export(resource, export_all, exclude, out, format):
    """
    This command allows you to export resources from Titan in either JSON or YAML format.
    You can specify the type of resource to export and the output filename for the exported data.

    Resource types are specified with snake case (eg. Warehouse => warehouse, NetworkRule => network_rule, etc.).
    You can also export all resources by using the --all flag. Specify multiple resource types by separating them with commas.

    Examples:

    \b
    # Export all database configurations to a file
    titan export --resource=database --out=databases.yml --format=yml

    \b
    # Export all resources
    titan export --all --out=titan.yml --format=yml

    \b
    # Export all resources except for users and roles
    titan export --all --exclude=user,role --out=titan.yml
    """

    if resource and export_all:
        raise click.UsageError("You can't specify both --resource and --all options at the same time.")

    resource_config = {}
    if resource:
        resource_config = export_resources(include=_parse_resources(resource))
    elif export_all:
        resource_config = export_resources(exclude=_parse_resources(exclude))
    else:
        raise

    output = None
    if format == "json":
        output = json.dumps(resource_config, indent=2)
    elif format == "yml":
        output = yaml.dump(resource_config, sort_keys=False)
    else:
        raise ValueError(f"Unsupported format: {format}")

    if out:
        with open(out, "w") as f:
            f.write(output)
    else:
        print(output)


@titan_cli.command("connect")
def cli_connect():
    """Test the connection to Snowflake"""
    env_vars = get_env_vars()
    if not env_vars:
        raise click.UsageError("No environment variables found. Please set the environment variables and try again.")
    for key, value in env_vars.items():
        value_inspect = "********" if key in ["password", "mfa_passcode"] else value
        print(f"SNOWFLAKE_{key.upper()}={value_inspect}")
    session = connect()
    print(f"Connection successful as user {session.user}")


if __name__ == "__main__":
    titan_cli()
