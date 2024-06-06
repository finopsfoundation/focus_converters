from uuid import uuid4

import pandas as pd
import polars as pl
import pytest

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.configs.string_transform_args import StringSplitArgument
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
                "test_column_split": "a/a/aa/aaa",
            },
            {
                "a": 1,
                "test_column_lower": "B",
                "test_column_upper": "b",
                "test_column_title": "bb",
                "test_column_split": "a/b/bb/bbb",
            },
            {
                "a": 1,
                "test_column_lower": "C",
                "test_column_upper": "c",
                "test_column_title": "cc",
                "test_column_split": "a/c/cc/ccc",
            },
            {
                "a": 1,
                "test_column_lower": "D",
                "test_column_upper": "d",
                "test_column_title": "dd",
                "test_column_split": "a/d/dd/ddd",
            },
            {
                "a": 1,
                "test_column_lower": None,
                "test_column_upper": None,
                "test_column_split": None,
            },
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

    def test_string_split(self, string_sample_df):
        pl_df = pl.from_dataframe(string_sample_df).lazy()
        assert pl_df.dtypes[1] == pl.Utf8

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column_split",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.STRING_FUNCTIONS,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "steps": [
                            StringSplitArgument(operation_type="split", split_by="/"),
                        ]
                    },
                ),
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()

        split_values = list(modified_pl_df["PlaceHolder"])
        assert list(split_values[0]) == ["a", "a", "aa", "aaa"]
        assert list(split_values[1]) == ["a", "b", "bb", "bbb"]
        assert list(split_values[2]) == ["a", "c", "cc", "ccc"]
        assert list(split_values[3]) == ["a", "d", "dd", "ddd"]
        assert split_values[4] is None

    def test_string_split_pick_at_index(self, string_sample_df):
        pl_df = pl.from_dataframe(string_sample_df).lazy()
        assert pl_df.dtypes[1] == pl.Utf8

        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {
            sample_provider_name: [
                ConversionPlan(
                    column="test_column_split",
                    config_file_name="D001_S001.yaml",
                    plan_name="test-plan",
                    dimension_id=1,
                    priority=0,
                    conversion_type=STATIC_CONVERSION_TYPES.STRING_FUNCTIONS,
                    focus_column=FocusColumnNames.PLACE_HOLDER,
                    conversion_args={
                        "steps": [
                            StringSplitArgument(
                                operation_type="split",
                                split_by="/",
                                index=2,
                            ),
                        ]
                    },
                ),
            ]
        }
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()

        split_values = list(modified_pl_df["PlaceHolder"])
        print(split_values)
        assert split_values[0] == "aa"
        assert split_values[1] == "bb"
        assert split_values[2] == "cc"
        assert split_values[3] == "dd"
        assert split_values[4] is None
