from unittest import TestCase

import pandas as pd

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.conversion_functions.column_functions import ColumnFunctions
from focus_converter.conversion_functions.validations import ColumnValidator
from focus_converter.models.focus_column_names import FocusColumnNames
import polars as pl


class TestUnnestOperation(TestCase):
    # Test suite to test unnest operations

    def test_unnest_with_child_field_in_dataset(self):
        # test

        sample_df = pd.DataFrame([{"nested_field": {"child": "a0"}}])
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr)

        self.assertEqual(list(pl_df.collect()["unnested"]), ["a0"])

    def test_unnest_with_child_with_list_default_behaviour(self):
        # tests default nesting behaviour to assume struct child is an object and selects first value

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={"children_type": "list"},
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 2)

    def test_unnest_with_child_with_list_first(self):
        # tests nesting behaviour to assume struct child is an object and selects first value

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={
                    "children_type": "list",
                    "aggregation_operation": "first",
                },
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 2)

    def test_unnest_with_child_with_list_last(self):
        # tests nesting behaviour to assume struct child is an object and selects last value

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={
                    "children_type": "list",
                    "aggregation_operation": "last",
                },
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 0)

    def test_unnest_with_child_with_list_sum(self):
        # tests nesting behaviour to assume struct child is an object and selects sum of values

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={
                    "children_type": "list",
                    "aggregation_operation": "sum",
                },
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 6)

    def test_unnest_with_child_with_list_mean(self):
        # tests nesting behaviour to assume struct child is an object and selects mean of values

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={
                    "children_type": "list",
                    "aggregation_operation": "mean",
                },
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 2)

    def test_unnest_with_child_with_list_min(self):
        # tests nesting behaviour to assume struct child is an object and selects min of values

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={
                    "children_type": "list",
                    "aggregation_operation": "min",
                },
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 0)

    def test_unnest_with_child_with_list_max(self):
        # tests nesting behaviour to assume struct child is an object and selects max of values

        sample_df = pd.DataFrame(
            [{"nested_field": [{"child": 2}, {"child": 4}, {"child": 0}]}]
        )
        pl_df = pl.from_pandas(sample_df).lazy()

        expr = ColumnFunctions.unnest(
            plan=ConversionPlan(
                column="nested_field.child",
                config_file_name="D0001-S000.yaml",
                plan_name="test-plan",
                dimension_id=1,
                priority=0,
                conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
                focus_column=FocusColumnNames.PROVIDER,
                conversion_args={
                    "children_type": "list",
                    "aggregation_operation": "max",
                },
            ),
            column_alias="unnested",
            column_validator=ColumnValidator(),
        )
        pl_df = pl_df.with_columns(expr).collect()
        unnested_values = list(pl_df["unnested"])
        self.assertEqual(len(unnested_values), 1)
        self.assertEqual(unnested_values[0], 4)
