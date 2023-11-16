from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
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


# noinspection DuplicatedCode
class TestSetColumnDTypes(TestCase):
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
