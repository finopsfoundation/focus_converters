import pathlib
import tempfile
from unittest import TestCase

from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats
from focus_converter.utils.profiler import Profiler


class TestOCISampleCSV(TestCase):
    def test_sample_csv_dataset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            export_path = pathlib.Path(temp_dir) / "oci_sample_csv_dataset"

            converter = FocusConverter(
                column_prefix=None  # Optional column prefix if needed else can be set to None
            )
            converter.load_provider_conversion_configs()
            converter.load_data(
                data_path="tests/provider_config_tests/oci/reports_cost-csv_0000000030000269.csv",
                data_format=DataFormats.CSV,
                parquet_data_format=None,
            )
            converter.configure_data_export(
                export_path=export_path,
                export_include_source_columns=False,
            )
            converter.prepare_horizontal_conversion_plan(provider="oci")
            self.execute_converter(converter)
    
    @Profiler(csv_format=True)    
    def execute_converter(self, converter):
        converter.convert()