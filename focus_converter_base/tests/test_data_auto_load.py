import os.path
import tempfile
from uuid import uuid4

import pandas as pd

from focus_converter.common.cli_options import DATA_FORMAT_OPTION
from focus_converter.data_loaders.provider_sensor import ProviderSensor


class TestDataAutoLoad:
    def test_provider_sense(self):
        """Tests correct provider is loaded correctly."""

        for provider_name, sample_data_file_path in [
            (
                "aws-cur",
                "tests/provider_config_tests/aws/sample-anonymous-aws-export-dataset.csv",
            ),
            (
                "azure",
                "tests/provider_config_tests/azure/sample-anonymous-ea-export-dataset.csv",
            ),
            (
                "oci",
                "tests/provider_config_tests/oci/reports_cost-csv_0000000030000269.csv",
            ),
        ]:
            provider_sensor = ProviderSensor(base_path=sample_data_file_path)
            provider_sensor.load()

            assert provider_sensor.provider == provider_name
            assert provider_sensor.data_format == DATA_FORMAT_OPTION.CSV

    def test_data_format_parquet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sample_df = [
                {"line_item_unblended_cost": 1},
                {"line_item_unblended_cost": 2},
            ]
            temp_file = os.path.join(temp_dir, f"{uuid4()}.parquet")
            df = pd.DataFrame(sample_df)
            df.to_parquet(temp_file)

            provider_sensor = ProviderSensor(base_path=temp_file)
            provider_sensor.load()

    def test_data_format_parquet_fragments(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sample_df = [
                {"line_item_unblended_cost": 1},
                {"line_item_unblended_cost": 2},
            ]
            temp_file = os.path.join(temp_dir, f"{uuid4()}.parquet")
            df = pd.DataFrame(sample_df)
            df.to_parquet(temp_file)

            sample_df = [
                {"line_item_unblended_cost": 3},
                {"line_item_unblended_cost": 4},
            ]
            temp_file = os.path.join(temp_dir, f"{uuid4()}.parquet")
            df = pd.DataFrame(sample_df)
            df.to_parquet(temp_file)

            provider_sensor = ProviderSensor(base_path=temp_dir)
            provider_sensor.load()

    def test_data_load_csv(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sample_df = [
                {"line_item_unblended_cost": 1},
                {"line_item_unblended_cost": 2},
            ]
            temp_file = os.path.join(temp_dir, f"{uuid4()}.csv")
            df = pd.DataFrame(sample_df)
            df.to_csv(temp_file)

            provider_sensor = ProviderSensor(base_path=temp_file)
            provider_sensor.load()
