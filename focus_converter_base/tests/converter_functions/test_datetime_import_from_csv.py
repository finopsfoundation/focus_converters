import os
import tempfile
from unittest import TestCase

import pandas as pd
import polars as pl

from focus_converter.data_loaders.data_loader import (
    DataFormats,
    DataLoader,
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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, "test.csv")
            test_df.to_csv(temp_file, index=False)
            loaded_df = pd.read_csv(temp_file, parse_dates=["date_column"])

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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, "test.csv")
            test_df.to_csv(temp_file, index=False)

            data_loader = DataLoader(
                data_path=temp_file,
                data_format=DataFormats.CSV,
                parquet_data_format=ParquetDataFormat.FILE,
            )
            lazy_frame = list(data_loader.load_csv())[0]
            self.assertEqual(
                lazy_frame.collect()["date_column"].dtype,
                pl.Utf8,
            )
