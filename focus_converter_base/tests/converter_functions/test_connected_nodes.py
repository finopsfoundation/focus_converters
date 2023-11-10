import re
import tempfile
from unittest import TestCase

import polars as pl

from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats, ParquetDataFormat


class TestConnectedNodes(TestCase):
    @staticmethod
    def __genetic_fnc_connected_nodes__(provider):
        # the only terminal node should be FOCUS column, ensuring that all columns are transformed

        converter = FocusConverter()
        converter.load_provider_conversion_configs()
        converter.prepare_horizontal_conversion_plan(provider=provider)

    def test_validate_aws_plan(self):
        self.__genetic_fnc_connected_nodes__(provider="aws")

    def test_validate_gcp_plan(self):
        self.__genetic_fnc_connected_nodes__(provider="gcp")

    def test_validate_azure_plan(self):
        self.__genetic_fnc_connected_nodes__(provider="azure")

    def test_validate_oci_plan(self):
        self.__genetic_fnc_connected_nodes__(provider="oci")

    def test_columns_check(self):
        lazy_frame = pl.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]}).lazy()

        with tempfile.NamedTemporaryFile() as file:
            lazy_frame.collect().write_parquet(file.name)

            converter = FocusConverter(column_prefix=None)
            converter.load_provider_conversion_configs()
            converter.prepare_horizontal_conversion_plan(provider="gcp")
            converter.load_data(
                data_path=file.name,
                data_format=DataFormats.PARQUET,
                parquet_data_format=ParquetDataFormat.DATASET,
            )
            converter.configure_data_export(
                export_path="/tmp/converted_aws",
                export_include_source_columns=False,
            )

            with self.assertRaises(ValueError) as cm:
                converter.convert()
            self.assertTrue(
                re.match(r"Column\(s\) '.*' not found in data", str(cm.exception))
            )
