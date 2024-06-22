import click
import json
import yaml

from titan.blueprint import print_plan, dump_plan
from titan.enums import ResourceType
from titan.operations.export import export_resources
from titan.operations.blueprint import blueprint_plan, blueprint_apply


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
    """titan[core] helps you manage your Snowflake environment."""
    pass


@titan_cli.command()
@click.option("--config", "config_file", type=str, help="Path to configuration YAML file.")
# @click.option("--resources", type=str, help="Specify resources to include in the plan.")
@click.option("--json", "json_output", is_flag=True, help="Output plan in machine-readable JSON format.")
@click.option("--out", "output_file", type=str, help="Output filename for the plan.")
@click.option(
    "--run_mode", type=click.Choice(["CREATE-OR-UPDATE"]), default="CREATE-OR-UPDATE", help="Run mode for the plan."
)
def plan(config_file, json_output, output_file, run_mode):
    """Generate an execution plan based on your configuration"""
    config = load_config(config_file)
    plan_obj = blueprint_plan(config, run_mode)
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


@titan_cli.command()
@click.option("--config", "config_file", type=str, help="Path to configuration YAML file.")
# @click.option("--resources", type=str, help="Specify resources to include in the apply.")
@click.option("--plan", "plan_file", type=str, help="Path to plan JSON file.")
@click.option(
    "--run_mode", type=click.Choice(["CREATE-OR-UPDATE"]), default="CREATE-OR-UPDATE", help="Run mode for the apply."
)
@click.option("--dry-run", is_flag=True, help="Perform a dry run without applying changes.")
def apply(config_file, plan_file, run_mode, dry_run):
    """Apply a plan to Titan resources"""
    if config_file and plan_file:
        raise click.UsageError("Cannot specify both --config and --plan.")
    if not config_file and not plan_file:
        raise click.UsageError("Either --config or --plan must be specified.")
    if config_file:
        config = load_config(config_file)
        plan_obj = blueprint_plan(config, run_mode)
        blueprint_apply(plan_obj, run_mode, dry_run)
    else:
        plan_obj = load_plan(plan_file)
        blueprint_apply(plan_obj, run_mode, dry_run)


@titan_cli.command()
@click.option("--resource", type=str, help="Specify the type of resource to export.")
@click.option("--out", type=str, help="Output filename for the exported data.")
@click.option("--format", type=click.Choice(["json", "yml"]), default="yml", help="Specify the output format.")
def export(resource, out, format):
    """Export Titan resources"""
    # Implementation for exporting resources
    resource_type = ResourceType(resource)
    resources = export_resources(resource_type)
    output = None
    if format == "json":
        output = json.dumps(resources, indent=2)
    elif format == "yml":
        output = yaml.dump(resources)

    if out:
        with open(out, "w") as f:
            f.write(output)
    else:
        print(output)


if __name__ == "__main__":
    titan_cli()
