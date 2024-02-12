from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl
import pytest

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames


@pytest.fixture(scope="function")
def string_sample_df():
    return pd.DataFrame(
        [
            {
                "a": 1,
                "test_column_lower": "A",
                "test_column_upper": "a",
                "test_column_title": "aa",
            },
            {
                "a": 1,
                "test_column_lower": "B",
                "test_column_upper": "b",
                "test_column_title": "bb",
            },
            {
                "a": 1,
                "test_column_lower": "C",
                "test_column_upper": "c",
                "test_column_title": "cc",
            },
            {
                "a": 1,
                "test_column_lower": "D",
                "test_column_upper": "d",
                "test_column_title": "dd",
            },
            {"a": 1, "test_column_lower": None, "test_column_upper": None},
        ]
    )


class TestStringFunctions:
    def test_lower(self, string_sample_df):
        pl_df = pl.from_dataframe(string_sample_df).lazy()
        assert pl_df.dtypes[1] == pl.Utf8

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column_lower",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.STRING_FUNCTIONS,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "steps": [
                            "lower",
                        ]
                    },
                ),
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        assert list(modified_pl_df["PlaceHolder"]) == ["a", "b", "c", "d", None]

    def test_upper(self, string_sample_df):
        pl_df = pl.from_dataframe(string_sample_df).lazy()
        assert pl_df.dtypes[1] == pl.Utf8

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column_upper",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.STRING_FUNCTIONS,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "steps": [
                            "upper",
                        ]
                    },
                ),
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        assert list(modified_pl_df["PlaceHolder"]) == ["A", "B", "C", "D", None]

    def test_title(self, string_sample_df):
        pl_df = pl.from_dataframe(string_sample_df).lazy()
        assert pl_df.dtypes[1] == pl.Utf8

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column_title",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.STRING_FUNCTIONS,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "steps": [
                            "title",
                        ]
                    },
                ),
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        assert list(modified_pl_df["PlaceHolder"]) == [
            "Aa",
            "Bb",
            "Cc",
            "Dd",
            None,
        ]
