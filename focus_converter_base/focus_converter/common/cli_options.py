import typer
from typing_extensions import Annotated

from focus_converter.data_loaders.data_loader import DataFormats, ParquetDataFormat

PROVIDER_OPTION = Annotated[
    str,
    typer.Option(help="Target provider"),
]

EXPORT_INCLUDE_SOURCE_COLUMNS = Annotated[
    bool,
    typer.Option(help="Include source columns", rich_help_panel="Data Export"),
]
EXPORT_PATH_OPTION = Annotated[
    str, typer.Option(help="Target data path", rich_help_panel="Data Export")
]

DATA_PATH = Annotated[
    str, typer.Option(help="Source data path", rich_help_panel="Source Data")
]
DATA_FORMAT_OPTION = Annotated[
    DataFormats, typer.Option(help="Data format", rich_help_panel="Source Data")
]
PARQUET_DATA_FORMAT_OPTION = Annotated[
    ParquetDataFormat,
    typer.Option(help="Parquet data format", rich_help_panel="Source Data"),
]

PLAN_GRAPH_PATH = Annotated[
    str,
    typer.Option(
        help="Path to save plan graph",
        rich_help_panel="Plan Graph",
    ),
]
