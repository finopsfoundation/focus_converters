import io
import json
import os
from sys import argv

import typer
from focus_validator.validator import Validator
from PIL import Image
from rich import print
from typing_extensions import Annotated

from focus_converter.common.cli_options import (
    DATA_FORMAT_OPTION,
    DATA_PATH,
    EXPORT_INCLUDE_SOURCE_COLUMNS,
    EXPORT_PATH_OPTION,
    PARQUET_DATA_FORMAT_OPTION,
    PLAN_GRAPH_PATH,
    PROVIDER_OPTION,
)
from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats
from focus_converter.data_loaders.provider_sensor import ProviderSensor

app = typer.Typer(name="FOCUS converters", add_completion=False)


@app.command(
    "convert-auto",
    help="Converts source cost data to FOCUS format automatically by reading data source to compute right provider and data format",
)
def main_auto(
    data_path: DATA_PATH,
    export_path: EXPORT_PATH_OPTION,
    export_include_source_columns: EXPORT_INCLUDE_SOURCE_COLUMNS = True,
    column_prefix: Annotated[
        str,
        typer.Option(
            help="Optional prefix to add to generated column names",
            rich_help_panel="Column Prefix",
        ),
    ] = (None,),
    converted_column_prefix: Annotated[
        str,
        typer.Option(
            help="Optional prefix to add to generated column names",
            rich_help_panel="Column Prefix",
        ),
    ] = ((None,),),
    validate: Annotated[
        bool,
        typer.Option(
            help="Validate generated data to match FOCUS spec.",
            rich_help_panel="Validation",
        ),
    ] = False,
):
    provider_sensor = ProviderSensor(base_path=data_path)
    provider_sensor.load()

    converter = FocusConverter(
        column_prefix=column_prefix, converted_column_prefix=converted_column_prefix
    )
    converter.load_provider_conversion_configs()
    converter.load_data(
        data_path=data_path,
        data_format=provider_sensor.data_format,
        parquet_data_format=provider_sensor.parquet_data_format,
    )
    converter.configure_data_export(
        export_path=export_path,
        export_include_source_columns=export_include_source_columns,
    )
    converter.prepare_horizontal_conversion_plan(provider=provider_sensor.provider)
    converter.convert()

    if validate:
        for segment_file_name in os.listdir(export_path):
            file_path = os.path.join(export_path, segment_file_name)
            validator = Validator(
                data_filename=file_path,
                output_type="console",
                output_destination=None,
            )
            validator.load()
            validator.validate()
            break


@app.command("convert", help="Converts source cost data to FOCUS format")
def main(
    provider: PROVIDER_OPTION,
    export_path: EXPORT_PATH_OPTION,
    data_format: DATA_FORMAT_OPTION,
    data_path: DATA_PATH,
    parquet_data_format: PARQUET_DATA_FORMAT_OPTION = None,
    export_include_source_columns: EXPORT_INCLUDE_SOURCE_COLUMNS = True,
    column_prefix: Annotated[
        str,
        typer.Option(
            help="Optional prefix to add to generated column names",
            rich_help_panel="Column Prefix",
        ),
    ] = (None,),
    converted_column_prefix: Annotated[
        str,
        typer.Option(
            help="Optional prefix to add to generated column names",
            rich_help_panel="Column Prefix",
        ),
    ] = ((None,),),
    validate: Annotated[
        bool,
        typer.Option(
            help="Validate generated data to match FOCUS spec.",
            rich_help_panel="Validation",
        ),
    ] = False,
):
    # compute function for conversion

    if data_format == DataFormats.PARQUET and parquet_data_format is None:
        raise typer.BadParameter("parquet_data_format required")

    converter = FocusConverter(
        column_prefix=column_prefix, converted_column_prefix=converted_column_prefix
    )
    converter.load_provider_conversion_configs()
    converter.load_data(
        data_path=data_path,
        data_format=data_format,
        parquet_data_format=parquet_data_format,
    )
    converter.configure_data_export(
        export_path=export_path,
        export_include_source_columns=export_include_source_columns,
    )
    converter.prepare_horizontal_conversion_plan(provider=provider)
    converter.convert()

    if validate:
        for segment_file_name in os.listdir(export_path):
            file_path = os.path.join(export_path, segment_file_name)
            validator = Validator(
                data_filename=file_path,
                output_type="console",
                output_destination=None,
            )
            validator.load()
            validator.validate()
            break


@app.command("explain", help="Show conversion plan and saves a graph as an image")
def explain(provider: PROVIDER_OPTION, image_path: PLAN_GRAPH_PATH):
    # function to show conversion plan
    converter = FocusConverter()
    converter.load_provider_conversion_configs()
    converter.prepare_horizontal_conversion_plan(provider=provider)

    image = Image.open(io.BytesIO(converter.explain()))
    image.save(image_path)


@app.command("list-providers", help="List available providers")
def list_providers():
    converter = FocusConverter()
    converter.load_provider_conversion_configs()
    print(json.dumps({"providers": list(converter.plans.keys())}, indent=4))


if __name__ == "__main__":
    if len(argv) == 1:
        # if no command specified, let the utility print the whole help message
        app(["--help"])
    else:
        app()
