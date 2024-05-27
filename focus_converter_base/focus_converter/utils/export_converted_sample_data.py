import os
import tempfile
from typing import Annotated

import polars as pl
import pyarrow.dataset as ds
import typer

from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats

app = typer.Typer(name="FOCUS export converted sample data", add_completion=False)


class GenerateSampleConvertedFOCUSDataset:
    @staticmethod
    def __helper__(output_dir, test_dataset_path, provider):
        with tempfile.TemporaryDirectory() as temp_dir:
            converter = FocusConverter()
            converter.load_provider_conversion_configs()
            converter.load_data(
                data_path=test_dataset_path,
                data_format=DataFormats.CSV,
            )
            converter.configure_data_export(
                export_path=os.path.join(temp_dir, "data"),
                export_include_source_columns=False,
            )
            converter.prepare_horizontal_conversion_plan(provider=provider)
            converter.convert()

            csv_output_path = os.path.join(output_dir, f"{provider}.csv")
            dataset = ds.dataset(temp_dir, format="parquet")
            df = pl.scan_pyarrow_dataset(dataset).collect()
            df.write_csv(csv_output_path)

            written_df = pl.scan_csv(csv_output_path).collect()

            # ensure that written dataframe has column and rows, greater than 0
            assert written_df.shape[0] > 0
            assert written_df.shape[1] > 0

    @classmethod
    def generate_aws_sample_dataset(cls, output_dir):
        cls.__helper__(
            output_dir=output_dir,
            test_dataset_path="tests/provider_config_tests/aws/sample-anonymous-aws-export-dataset.csv",
            provider="aws-cur",
        )

    @classmethod
    def generate_oci_sample_dataset(cls, output_dir):
        cls.__helper__(
            output_dir=output_dir,
            test_dataset_path="tests/provider_config_tests/oci/reports_cost-csv_0000000030000269.csv",
            provider="oci",
        )

    @classmethod
    def generate_azure_sample_dataset(cls, output_dir):
        cls.__helper__(
            output_dir=output_dir,
            test_dataset_path="tests/provider_config_tests/azure/sample-anonymous-ea-export-dataset.csv",
            provider="azure",
        )


@app.command("export-focus-dataset", help="Converts source cost data to FOCUS format")
def export_converted_sample_data(
    output_dir: Annotated[
        str,
        typer.Option(help="Write provider csvs to this path."),
    ]
):
    GenerateSampleConvertedFOCUSDataset.generate_aws_sample_dataset(
        output_dir=output_dir
    )
    GenerateSampleConvertedFOCUSDataset.generate_oci_sample_dataset(
        output_dir=output_dir
    )
    GenerateSampleConvertedFOCUSDataset.generate_azure_sample_dataset(
        output_dir=output_dir
    )


if __name__ == "__main__":
    app()
