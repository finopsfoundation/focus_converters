import os
import tempfile
from datetime import datetime, date
from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl
import polars.exceptions
import multiprocessing

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataLoader, DataFormats
from focus_converter.models.focus_column_names import FocusColumnNames

RENAME_SAMPLE_PLAN = ConversionPlan(
    column="test_column",
    config_file_name="D001_S001.yaml",
    plan_name="test-plan",
    dimension_id=1,
    priority=0,
    conversion_type=STATIC_CONVERSION_TYPES.RENAME_COLUMN,
    focus_column=FocusColumnNames.PROVIDER,
)


multiprocessing.set_start_method("spawn", force=True)


# noinspection DuplicatedCode
class TestSetColumnDTypes(TestCase):
    """
    This test suite ensures that cast functions work without any issue for the known datatypes.
    For datetime and date columns, the expectation is if the column is in iso standard, dataframe library
    will pick up i64 as the type and then do a successful cast on it else raise an exception and let the user
    know that it cannot be automatically converted.

    Also for the cases where date cannot be automatically converted, the expectation would be to write a parser
    that can.
    """

    @classmethod
    def setUpClass(cls):
        cls.random_id = radom_id = str(uuid4())

    def test_conversion_args(self):
        with self.assertRaises(ValueError) as cm:
            ConversionPlan(
                column="test_column",
                config_file_name="D001_S001.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                focus_column=FocusColumnNames.PLACE_HOLDER,
            )
        self.assertIn(
            "Input should be a valid dictionary or instance of SetColumnDTypesConversionArgs",
            str(cm.exception),
        )

    def test_dtypes_cast_from_string_to_float(self):
        df = pd.DataFrame(
            [
                {"a": 1, "test_column": "2"},
                {"a": 1, "test_column": "3"},
                {"a": 1, "test_column": "4"},
                {"a": 1, "test_column": "5"},
                {"a": 1, "test_column": None},
            ]
        )
        pl_df = pl.from_dataframe(df).lazy()
        self.assertEqual(pl_df.dtypes[1], pl.Utf8)

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "dtype_args": [
                            {"dtype": "float", "column_name": "test_column"},
                        ]
                    },
                ),
                RENAME_SAMPLE_PLAN,
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        self.assertEqual(modified_pl_df.dtypes[1], pl.Float64)
        self.assertEqual(
            list(modified_pl_df["test_column"]),
            [2.0, 3.0, 4.0, 5.0, None],
        )

    def test_dtypes_cast_from_string_to_int(self):
        df = pd.DataFrame(
            [
                {"a": 1, "test_column": "2"},
                {"a": 1, "test_column": "3"},
                {"a": 1, "test_column": "4"},
                {"a": 1, "test_column": "5"},
                {"a": 1, "test_column": None},
            ]
        )
        pl_df = pl.from_dataframe(df).lazy()
        self.assertEqual(pl_df.dtypes[1], pl.Utf8)

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "dtype_args": [
                            {"dtype": "int", "column_name": "test_column"},
                        ]
                    },
                ),
                RENAME_SAMPLE_PLAN,
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        self.assertEqual(modified_pl_df.dtypes[1], pl.Int64)
        self.assertEqual(
            list(modified_pl_df["test_column"]),
            [2.0, 3.0, 4.0, 5.0, None],
        )

    def test_dtype_cast_str_to_datetime(self):
        test_datetime = datetime(2021, 1, 1, 0, 0, 0)
        test_datetime_str = test_datetime.isoformat()

        df = pd.DataFrame(
            [
                {"a": 1, "test_column": test_datetime_str},
                {"a": 1, "test_column": test_datetime_str},
                {"a": 1, "test_column": test_datetime_str},
                {"a": 1, "test_column": test_datetime_str},
                {"a": 1, "test_column": None},
            ]
        )

        # write dataframe to a csv and then read it from the data loader to simulate default polars data load behaviour
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "test.csv")
            df.to_csv(temp_file_path)

            data_loader = DataLoader(
                data_path=temp_file_path,
                data_format=DataFormats.CSV,
            )
            pl_df = list(data_loader.load_csv())[0]
            self.assertEqual(pl_df.select("test_column").dtypes[0], pl.Utf8)

            sample_provider_name = str(uuid4())

            focus_converter = FocusConverter(column_prefix=None)
            focus_converter.plans = {
                sample_provider_name: [
                    ConversionPlan(
                        column="test_column",
                        config_file_name="D001_S001.yaml",
                        plan_name="test-plan",
                        dimension_id=1,
                        priority=0,
                        conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                        focus_column=FocusColumnNames.PLACE_HOLDER,
                        conversion_args={
                            "dtype_args": [
                                {"dtype": "datetime", "column_name": "test_column"},
                            ]
                        },
                    ),
                    RENAME_SAMPLE_PLAN,
                ]
            }
            focus_converter.prepare_horizontal_conversion_plan(
                provider=sample_provider_name
            )
            modified_pl_df = (
                focus_converter.__process_lazy_frame__(lf=pl_df)
                .select("test_column")
                .collect()
            )
        self.assertEqual(
            modified_pl_df.dtypes[0], pl.Datetime(time_unit="us", time_zone=None)
        )
        self.assertEqual(
            list(modified_pl_df["test_column"]),
            [test_datetime, test_datetime, test_datetime, test_datetime, None],
        )

    def test_dtype_cast_str_to_date_only(self):
        test_date = date(2021, 1, 1)
        test_date_str = test_date.isoformat()

        df = pd.DataFrame(
            [
                {"a": 1, "test_column": test_date_str},
                {"a": 1, "test_column": test_date_str},
                {"a": 1, "test_column": test_date_str},
                {"a": 1, "test_column": test_date_str},
                {"a": 1, "test_column": None},
            ]
        )

        # write dataframe to a csv and then read it from the data loader to simulate default polars data load behaviour
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "test.csv")
            df.to_csv(temp_file_path)

            data_loader = DataLoader(
                data_path=temp_file_path,
                data_format=DataFormats.CSV,
            )
            pl_df = list(data_loader.load_csv())[0]
            self.assertEqual(pl_df.select("test_column").dtypes[0], pl.Utf8)

            sample_provider_name = str(uuid4())

            focus_converter = FocusConverter(column_prefix=None)
            focus_converter.plans = {
                sample_provider_name: [
                    ConversionPlan(
                        column="test_column",
                        config_file_name="D001_S001.yaml",
                        plan_name="test-plan",
                        dimension_id=1,
                        priority=0,
                        conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                        focus_column=FocusColumnNames.PLACE_HOLDER,
                        conversion_args={
                            "dtype_args": [
                                {"dtype": "date", "column_name": "test_column"},
                            ]
                        },
                    ),
                    RENAME_SAMPLE_PLAN,
                ]
            }
            focus_converter.prepare_horizontal_conversion_plan(
                provider=sample_provider_name
            )
            modified_pl_df = (
                focus_converter.__process_lazy_frame__(lf=pl_df)
                .select("test_column")
                .collect()
            )
        self.assertEqual(modified_pl_df.dtypes[0], pl.Date)
        self.assertEqual(
            list(modified_pl_df["test_column"]),
            [test_date, test_date, test_date, test_date, None],
        )

    def test_dtype_cast_bad_str_to_datetime(self):
        # converter needs to raise exception when it cannot automatically cast to datetime or date field
        df = pd.DataFrame(
            [
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": None},
            ]
        )

        # write dataframe to a csv and then read it from the data loader to simulate default polars data load behaviour
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "test.csv")
            df.to_csv(temp_file_path)

            data_loader = DataLoader(
                data_path=temp_file_path,
                data_format=DataFormats.CSV,
            )
            pl_df = list(data_loader.load_csv())[0]
            self.assertEqual(pl_df.select("test_column").dtypes[0], pl.Utf8)

            sample_provider_name = str(uuid4())

            focus_converter = FocusConverter(column_prefix=None)
            focus_converter.plans = {
                sample_provider_name: [
                    ConversionPlan(
                        column="test_column",
                        config_file_name="D001_S001.yaml",
                        plan_name="test-plan",
                        dimension_id=1,
                        priority=0,
                        conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                        focus_column=FocusColumnNames.PLACE_HOLDER,
                        conversion_args={
                            "dtype_args": [
                                {"dtype": "datetime", "column_name": "test_column"},
                            ]
                        },
                    ),
                    RENAME_SAMPLE_PLAN,
                ]
            }
            focus_converter.prepare_horizontal_conversion_plan(
                provider=sample_provider_name
            )
            with self.assertRaises(polars.exceptions.ComputeError) as cm:
                focus_converter.__process_lazy_frame__(lf=pl_df).select(
                    "test_column"
                ).collect()

            self.assertIn(
                "could not find an appropriate format to parse dates, please define a format",
                str(cm.exception),
                str(cm.exception),
            )

    def test_dtype_cast_bad_str_to_date(self):
        # converter needs to raise exception when it cannot automatically cast to datetime or date field
        df = pd.DataFrame(
            [
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": "bad-date"},
                {"a": 1, "test_column": None},
            ]
        )

        # write dataframe to a csv and then read it from the data loader to simulate default polars data load behaviour
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "test.csv")
            df.to_csv(temp_file_path)

            data_loader = DataLoader(
                data_path=temp_file_path,
                data_format=DataFormats.CSV,
            )
            pl_df = list(data_loader.load_csv())[0]
            self.assertEqual(pl_df.select("test_column").dtypes[0], pl.Utf8)

            sample_provider_name = str(uuid4())

            focus_converter = FocusConverter(column_prefix=None)
            focus_converter.plans = {
                sample_provider_name: [
                    ConversionPlan(
                        column="test_column",
                        config_file_name="D001_S001.yaml",
                        plan_name="test-plan",
                        dimension_id=1,
                        priority=0,
                        conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                        focus_column=FocusColumnNames.PLACE_HOLDER,
                        conversion_args={
                            "dtype_args": [
                                {"dtype": "date", "column_name": "test_column"},
                            ]
                        },
                    ),
                    RENAME_SAMPLE_PLAN,
                ]
            }
            focus_converter.prepare_horizontal_conversion_plan(
                provider=sample_provider_name
            )
            with self.assertRaises(polars.exceptions.ComputeError) as cm:
                focus_converter.__process_lazy_frame__(lf=pl_df).select(
                    "test_column"
                ).collect()

            self.assertIn(
                "could not find an appropriate format to parse dates, please define a format",
                str(cm.exception),
            )

    def test_dtype_cast_datetime_to_datetime(self):
        # conversions need to be resilient to already converted columns
        test_datetime = datetime(2021, 1, 1, 0, 0, 0)

        df = pd.DataFrame(
            [
                {"a": 1, "test_column": test_datetime},
                {"a": 1, "test_column": test_datetime},
                {"a": 1, "test_column": test_datetime},
                {"a": 1, "test_column": test_datetime},
                {"a": 1, "test_column": None},
            ]
        )

        # write dataframe to a csv and then read it from the data loader to simulate default polars data load behaviour
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "test.parquet")
            df.to_parquet(temp_file_path)

            data_loader = DataLoader(
                data_path=temp_file_path,
                data_format=DataFormats.PARQUET,
            )
            pl_df = list(data_loader.load_parquet_file())[0]
            self.assertEqual(
                pl_df.select("test_column").dtypes[0],
                pl.Datetime(time_unit="ns", time_zone=None),
            )

            sample_provider_name = str(uuid4())

            focus_converter = FocusConverter(column_prefix=None)
            focus_converter.plans = {
                sample_provider_name: [
                    ConversionPlan(
                        column="test_column",
                        config_file_name="D001_S001.yaml",
                        plan_name="test-plan",
                        dimension_id=1,
                        priority=0,
                        conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                        focus_column=FocusColumnNames.PLACE_HOLDER,
                        conversion_args={
                            "dtype_args": [
                                {"dtype": "datetime", "column_name": "test_column"},
                            ]
                        },
                    ),
                    RENAME_SAMPLE_PLAN,
                ]
            }
            focus_converter.prepare_horizontal_conversion_plan(
                provider=sample_provider_name
            )
            modified_pl_df = (
                focus_converter.__process_lazy_frame__(lf=pl_df)
                .select("test_column")
                .collect()
            )
        self.assertEqual(
            modified_pl_df.dtypes[0], pl.Datetime(time_unit="ns", time_zone=None)
        )
        self.assertEqual(
            list(modified_pl_df["test_column"]),
            [test_datetime, test_datetime, test_datetime, test_datetime, None],
        )

    def test_dtype_cast_date_to_date(self):
        # conversions need to be resilient to already converted columns
        test_date = date(2021, 1, 1)

        df = pd.DataFrame(
            [
                {"a": 1, "test_column": test_date},
                {"a": 1, "test_column": test_date},
                {"a": 1, "test_column": test_date},
                {"a": 1, "test_column": test_date},
                {"a": 1, "test_column": None},
            ]
        )

        # write dataframe to a csv and then read it from the data loader to simulate default polars data load behaviour
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, "test.parquet")
            df.to_parquet(temp_file_path)

            data_loader = DataLoader(
                data_path=temp_file_path,
                data_format=DataFormats.PARQUET,
            )
            pl_df = list(data_loader.load_parquet_file())[0]
            self.assertEqual(pl_df.select("test_column").dtypes[0], pl.Date)

            sample_provider_name = str(uuid4())

            focus_converter = FocusConverter(column_prefix=None)
            focus_converter.plans = {
                sample_provider_name: [
                    ConversionPlan(
                        column="test_column",
                        config_file_name="D001_S001.yaml",
                        plan_name="test-plan",
                        dimension_id=1,
                        priority=0,
                        conversion_type=STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                        focus_column=FocusColumnNames.PLACE_HOLDER,
                        conversion_args={
                            "dtype_args": [
                                {"dtype": "date", "column_name": "test_column"},
                            ]
                        },
                    ),
                    RENAME_SAMPLE_PLAN,
                ]
            }
            focus_converter.prepare_horizontal_conversion_plan(
                provider=sample_provider_name
            )
            modified_pl_df = (
                focus_converter.__process_lazy_frame__(lf=pl_df)
                .select("test_column")
                .collect()
            )
        self.assertEqual(modified_pl_df.dtypes[0], pl.Date)
        self.assertEqual(
            list(modified_pl_df["test_column"]),
            [test_date, test_date, test_date, test_date, None],
        )
