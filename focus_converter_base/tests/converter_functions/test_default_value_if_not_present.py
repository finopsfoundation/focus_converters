from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames

DEFAULT_IF_MISSING_VALUE_SAMPLE_YAML = """
plan_name: sample
priority: 1
column: test_column
conversion_type: apply_default_if_column_missing
focus_column: Region
"""

RENAME_SAMPLE_PLAN = ConversionPlan(
    column="test_column",
    config_file_name="D001_S001.yaml",
    plan_name="test-plan",
    dimension_id=1,
    priority=0,
    conversion_type=STATIC_CONVERSION_TYPES.RENAME_COLUMN,
    focus_column=FocusColumnNames.PROVIDER,
)


class TestDefaultValueIfNotPresent(TestCase):
    def test_if_column_present_in_source(self):
        df = pd.DataFrame(
            [
                {"a": 1, "test_column": 2},
                {"a": 1, "test_column": 3},
                {"a": 1, "test_column": 4},
                {"a": 1, "test_column": 5},
            ]
        )
        pl_df = pl.from_dataframe(df).lazy()

        sample_provider_name = str(uuid4())
        test_plan = ConversionPlan(
            column="test_column",
            config_file_name="D001_S001.yaml",
            plan_name="test-plan",
            dimension_id=1,
            priority=0,
            conversion_type=STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING,
            focus_column=FocusColumnNames.PROVIDER,
            column_prefix="tmp_prefill",
        )

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {sample_provider_name: [test_plan, RENAME_SAMPLE_PLAN]}
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        print(modified_pl_df)
        assigned_value = list(modified_pl_df["Provider"])

        self.assertEqual(len(assigned_value), 4)
        self.assertEqual(list(assigned_value), [2, 3, 4, 5])

    def test_if_column_not_present_in_source(self):
        df = pd.DataFrame(
            [
                {"a": 1},
                {"a": 1},
                {"a": 1},
                {"a": 1},
            ]
        )
        pl_df = pl.from_dataframe(df).lazy()

        sample_provider_name = str(uuid4())
        test_plan = ConversionPlan(
            column="test_column",
            config_file_name="D001_S001.yaml",
            plan_name="test-plan",
            dimension_id=1,
            priority=0,
            conversion_type=STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING,
            focus_column=FocusColumnNames.PROVIDER,
            column_prefix="tmp_prefill",
        )

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {sample_provider_name: [test_plan, RENAME_SAMPLE_PLAN]}
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )

        modified_pl_df = focus_converter.__process_lazy_frame__(lf=pl_df).collect()
        assigned_value = list(modified_pl_df["Provider"])

        self.assertEqual(len(assigned_value), 4)
        self.assertEqual(list(assigned_value), [None, None, None, None])

    def test_uml_generation(self):
        test_plan = ConversionPlan(
            column="test_column",
            config_file_name="D001_S001.yaml",
            plan_name="test-plan",
            dimension_id=1,
            priority=1,
            conversion_type=STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING,
            focus_column=FocusColumnNames.PROVIDER,
            column_prefix="tmp_prefill",
        )

        sample_provider_name = str(uuid4())
        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {sample_provider_name: [test_plan, RENAME_SAMPLE_PLAN]}
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )
        uml = focus_converter.__column_validator__.generate_mermaid_uml()

        expected_output = f"graph TD;\n"
        expected_output += "\tSOURCE -- APPLY_DEFAULT_IF_COLUMN_MISSING:D001_S001.yaml --> test_column\n"
        expected_output += (
            "\ttest_column -- RENAME_COLUMN:D001_S001.yaml --> Provider\n"
        )
        expected_output += "\tProvider --> FOCUS_DATASET\n"
        self.assertEqual(uml, expected_output)
