import json
from typing import Any

import click
import yaml

from titan.blueprint import dump_plan
from titan.enums import RunMode, BlueprintScope
from titan.gitops import (
    collect_configs_from_path,
    collect_vars_from_environment,
    merge_configs,
    merge_vars,
    parse_resources,
)
from titan.operations.blueprint import blueprint_apply, blueprint_apply_plan, blueprint_plan
from titan.operations.connector import connect, get_env_vars
from titan.operations.export import export_resources


class RunModeParamType(click.ParamType):
    name = "run_mode"

    def convert(self, value, param, ctx):
        return RunMode(value)


class ScopeParamType(click.ParamType):
    name = "scope"

    def convert(self, value, param, ctx):
        return BlueprintScope(value)


class JsonParamType(click.ParamType):
    name = "json"

    def convert(self, value, param, ctx):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            self.fail(f"'{value}' is not a valid JSON string", param, ctx)


class CommaSeparatedListParamType(click.ParamType):
    name = "comma_separated_list"

    def convert(self, value, param, ctx):
        return parse_resources(value)


def load_plan(plan_file):
    with open(plan_file, "r") as f:
        plan = json.load(f)
    return plan


@click.group()
def titan_cli():
    """titan core helps you manage your Snowflake environment."""
    pass


# Shared click options


def config_path_option():
    return click.option(
        "--config",
        "config_path",
        type=str,
        help="Path to configuration YAML file or directory",
        metavar="<file_or_dir>",
    )


def vars_option():
    return click.option(
        "--vars",
        type=JsonParamType(),
        help="Dynamic values, specified as a JSON dictionary",
        metavar="<JSON_string>",
    )


def allowlist_option():
    return click.option(
        "--allowlist",
        type=CommaSeparatedListParamType(),
        help="List of resources types allowed in the plan. If not specified, all resources are allowed.",
        metavar="<resource_types>",
    )


def run_mode_option():
    return click.option(
        "--mode",
        "run_mode",
        type=RunModeParamType(),
        metavar="<run_mode>",
        show_default=True,
        help="Run mode",
    )


def scope_option():
    return click.option(
        "--scope",
        type=ScopeParamType(),
        help="Limit the scope of resources to a specific database or schema",
        metavar="<scope>",
    )


def database_option():
    return click.option(
        "--database",
        type=str,
        help="Database to limit the scope to",
        metavar="<database_name>",
    )


def schema_option():
    return click.option(
        "--schema",
        type=str,
        help="Schema to limit the scope to",
        metavar="<schema_name>",
    )


@titan_cli.command("plan", no_args_is_help=True)
@config_path_option()
@click.option("--json", "json_output", is_flag=True, help="Output plan in machine-readable JSON format")
@click.option("--out", "output_file", type=str, help="Write plan to a file", metavar="<filename>")
@vars_option()
@allowlist_option()
@run_mode_option()
@scope_option()
@database_option()
@schema_option()
def plan(config_path, json_output, output_file, vars: dict, allowlist, run_mode, scope, database, schema):
    """Generate an execution plan based on your configuration"""

    if not config_path:
        raise click.UsageError("--config is required")

    yaml_config: dict[str, Any] = {}
    configs = collect_configs_from_path(config_path)
    for config in configs:
        yaml_config = merge_configs(yaml_config, config[1])

    cli_config: dict[str, Any] = {}
    if vars:
        cli_config["vars"] = vars
    if run_mode:
        cli_config["run_mode"] = RunMode(run_mode)
    if allowlist:
        cli_config["allowlist"] = allowlist
    if scope:
        cli_config["scope"] = scope
    if database:
        cli_config["database"] = database
    if schema:
        cli_config["schema"] = schema

    env_vars = collect_vars_from_environment()
    if env_vars:
        cli_config["vars"] = merge_vars(cli_config.get("vars", {}), env_vars)

    plan_obj = blueprint_plan(yaml_config, cli_config)
    if output_file:
        with open(output_file, "w") as f:
            f.write(dump_plan(plan_obj, format="json"))
    else:
        output = None
        if json_output:
            output = dump_plan(plan_obj, format="json")
        else:
            output = dump_plan(plan_obj, format="text")
        print(output)


@titan_cli.command("apply", no_args_is_help=True)
@config_path_option()
@click.option("--plan", "plan_file", type=str, help="Path to plan JSON file", metavar="<filename>")
@vars_option()
@allowlist_option()
@run_mode_option()
@scope_option()
@database_option()
@schema_option()
@click.option("--dry-run", is_flag=True, help="When dry run is true, Titan will not make any changes to Snowflake")
def apply(config_path, plan_file, vars, allowlist, run_mode, scope, database, schema, dry_run):
    """Apply an execution plan to a Snowflake account"""

    if config_path and plan_file:
        raise click.UsageError("Cannot specify both --config and --plan.")
    if not config_path and not plan_file:
        raise click.UsageError("Either --config or --plan must be specified.")

    cli_config: dict[str, Any] = {}
    if vars:
        cli_config["vars"] = vars
    if run_mode:
        cli_config["run_mode"] = RunMode(run_mode)
    if dry_run:
        cli_config["dry_run"] = dry_run
    if allowlist:
        cli_config["allowlist"] = allowlist
    if scope:
        cli_config["scope"] = scope
    if database:
        cli_config["database"] = database
    if schema:
        cli_config["schema"] = schema

    env_vars = collect_vars_from_environment()
    if env_vars:
        cli_config["vars"] = merge_vars(cli_config.get("vars", {}), env_vars)

    if config_path:
        yaml_config: dict[str, Any] = {}
        configs = collect_configs_from_path(config_path)
        for config in configs:
            yaml_config = merge_configs(yaml_config, config[1])
        blueprint_apply(yaml_config, cli_config)
    elif plan_file:
        plan_obj = load_plan(plan_file)
        blueprint_apply_plan(plan_obj, cli_config)
    else:
        raise Exception("No config or plan file specified")


@titan_cli.command("export", context_settings={"show_default": True}, no_args_is_help=True)
@click.option(
    "--resource",
    "resources",
    type=CommaSeparatedListParamType(),
    help="The resource types to export",
    metavar="<resource_types>",
)
@click.option("--all", "export_all", is_flag=True, help="Export all resources")
@click.option(
    "--exclude",
    "exclude_resources",
    type=CommaSeparatedListParamType(),
    help="Exclude resources, used with --all",
    metavar="<resource_types>",
)
@click.option("--out", type=str, help="Write exported config to a file", metavar="<filename>")
@click.option("--format", type=click.Choice(["json", "yml"]), default="yml", help="Output format")
def export(resources, export_all, exclude_resources, out, format):
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

    if resources and export_all:
        raise click.UsageError("You can't specify both --resource and --all options at the same time.")

    resource_config: dict[str, Any] = {}
    if resources:
        resource_config = export_resources(include=resources)
    elif export_all:
        resource_config = export_resources(exclude=exclude_resources)
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
