import pathlib
import tempfile

import polars as pl
import pyarrow.dataset as ds

from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats


class TestAWSSampleCSV:
    def test_sample_csv_dataset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = pathlib.Path(temp_dir).joinpath("aws_sample_csv_dataset")

            converter = FocusConverter(
                column_prefix=None  # Optional column prefix if needed else can be set to None
            )
            converter.load_provider_conversion_configs()
            converter.load_data(
                data_path="tests/provider_config_tests/aws/sample-anonymous-aws-export-dataset.csv",
                data_format=DataFormats.CSV,
                parquet_data_format=None,
            )
            converter.configure_data_export(
                export_path=export_path,
                export_include_source_columns=False,
            )
            converter.prepare_horizontal_conversion_plan(provider="aws-cur")

            # Run the method to be profiled
            converter.convert()

            dataset = ds.dataset(temp_dir, format="parquet")
            df = pl.scan_pyarrow_dataset(dataset).collect()
            assert len(df.columns) > 0
