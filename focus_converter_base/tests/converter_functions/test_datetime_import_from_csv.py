import tempfile
from unittest import TestCase

import pandas as pd
from polars import Datetime

from focus_converter.data_loaders.data_loader import (
    DataLoader,
    DataFormats,
    ParquetDataFormat,
)


class TestDatetimeImportFromCSV(TestCase):
    """
    Test the import of datetime columns from CSV files.
    """

    def test_pandas_native_behaviour(self):
        """
        Test the native behaviour of pandas when importing datetime columns from CSV files.
        """

        test_df = pd.DataFrame(
            [
                {"date_column": "2023-06-01 00:00:00.000000 UTC"},
                {"date_column": "2023-06-01 00:00:00.000000 UTC"},
            ]
        )

        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
            test_df.to_csv(temp_file.name, index=False)
            loaded_df = pd.read_csv(temp_file.name, parse_dates=["date_column"])

        self.assertEqual(loaded_df["date_column"].dtype, "datetime64[ns, UTC]")

    def test_data_loader(self):
        """
        Test the behaviour of the data loader when importing datetime columns from CSV files.
        """

        test_df = pd.DataFrame(
            [
                {"date_column": "2023-06-01 00:00:00.000000 UTC"},
                {"date_column": "2023-06-01 00:00:00.000000 UTC"},
            ]
        )

        with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
            test_df.to_csv(temp_file.name, index=False)

            data_loader = DataLoader(
                data_path=temp_file.name,
                data_format=DataFormats.CSV,
                parquet_data_format=ParquetDataFormat.FILE,
            )
            lazy_frame = list(data_loader.load_csv())[0]
            self.assertEqual(
                lazy_frame.collect()["date_column"].dtype,
                Datetime(time_unit="us", time_zone="UTC"),
            )
