import os
import tempfile
from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl
from jinja2 import Template
from pydantic import ValidationError

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions.column_functions import ColumnFunctions
from focus_converter.conversion_functions.validations import ColumnValidator

STATIC_VALUE_SAMPLE_TEMPLATE_YAML = """
plan_name: sample
priority: 1
column: test_column
conversion_type: static_value
focus_column: RegionId
conversion_args:
    static_value: {{ random_static_value }}
"""

STATIC_VALUE_SAMPLE_TEMPLATE_MISSING_VALUE_YAML = """
plan_name: sample
priority: 1
column: test_column
conversion_type: static_value
focus_column: RegionId
"""

STATIC_VALUE_SAMPLE_TEMPLATE_YAML_JINJA = Template(STATIC_VALUE_SAMPLE_TEMPLATE_YAML)


# noinspection DuplicatedCode
class TestStaticValuePlan(TestCase):
    # This test suite tests for static value plan where a value is assigned to a column

    def test_static_value_defined(self):
        # this function tests conversion args validation

        with tempfile.TemporaryDirectory() as temp_dir:
            sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

            with open(sample_file_path, "w") as fd:
                fd.write(STATIC_VALUE_SAMPLE_TEMPLATE_MISSING_VALUE_YAML)

            with self.assertRaises(ValidationError) as cm:
                ConversionPlan.load_yaml(sample_file_path)
            self.assertEqual(len(cm.exception.errors()), 1)
            self.assertEqual(cm.exception.errors()[0]["loc"], ("conversion_args",))

    def test_static_value(self):
        # this test applies a static value on a randomly created dataframe.

        df = pd.DataFrame(
            [
                {"a": 1, "b": 2},
                {"a": 1, "b": 2},
                {"a": 1, "b": 2},
                {"a": 1, "b": 2},
            ]
        )
        pl_df = pl.from_dataframe(df).lazy()

        random_column_alias = str(uuid4())
        random_static_value = str((uuid4()))

        generated_yaml = STATIC_VALUE_SAMPLE_TEMPLATE_YAML_JINJA.render(
            random_static_value=random_static_value
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

            with open(sample_file_path, "w") as fd:
                fd.write(generated_yaml)

            conversion_plan = ConversionPlan.load_yaml(sample_file_path)
            sample_col = ColumnFunctions.assign_static_value(
                plan=conversion_plan,
                column_alias=random_column_alias,
                column_validator=ColumnValidator(),
            )

            modified_pl_df = pl_df.with_columns([sample_col]).collect()
            assigned_value = set(modified_pl_df[random_column_alias])

            self.assertEqual(len(assigned_value), 1)
            self.assertEqual(list(assigned_value)[0], random_static_value)
