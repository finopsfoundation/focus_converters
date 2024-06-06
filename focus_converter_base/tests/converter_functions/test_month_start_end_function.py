import os
import tempfile
from datetime import date, datetime
from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions.datetime_functions import (
    DateTimeConversionFunctions,
)
from focus_converter.conversion_functions.validations import ColumnValidator

START_MONTH_SAMPLE_TEMPLATE_YAML = """
plan_name: sample
priority: 1
column: test_column
conversion_type: month_start
focus_column: BillingPeriodStart
"""

END_MONTH_SAMPLE_TEMPLATE_YAML = """
plan_name: sample
priority: 1
column: test_column
conversion_type: month_end
focus_column: BillingPeriodEnd
"""


class TestMonthStartFunction(TestCase):
    """
    Test the month_start function
    """

    def test_month_start(self):
        df = pd.DataFrame(
            [{"test_column": datetime(year=2020, month=1, day=25, hour=12)}]
        )

        pl_df = pl.from_pandas(df).lazy()
        random_column_alias = str(uuid4())

        with tempfile.TemporaryDirectory() as temp_dir:
            sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

            with open(sample_file_path, "w") as fd:
                fd.write(START_MONTH_SAMPLE_TEMPLATE_YAML)

            conversion_plan = ConversionPlan.load_yaml(sample_file_path)
            sample_col = DateTimeConversionFunctions.month_start(
                plan=conversion_plan,
                column_alias=random_column_alias,
                column_validator=ColumnValidator(),
            )

            modified_pl_df = pl_df.with_columns([sample_col]).collect()
            converted_value = set(modified_pl_df[random_column_alias])

            self.assertEqual(len(converted_value), 1)
            self.assertEqual(list(converted_value)[0], date(year=2020, month=1, day=1))

    def test_month_end(self):
        df = pd.DataFrame(
            [{"test_column": datetime(year=2020, month=1, day=25, hour=12)}]
        )

        pl_df = pl.from_pandas(df).lazy()
        random_column_alias = str(uuid4())

        with tempfile.TemporaryDirectory() as temp_dir:
            sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

            with open(sample_file_path, "w") as fd:
                fd.write(END_MONTH_SAMPLE_TEMPLATE_YAML)

            conversion_plan = ConversionPlan.load_yaml(sample_file_path)
            sample_col = DateTimeConversionFunctions.month_end(
                plan=conversion_plan,
                column_alias=random_column_alias,
                column_validator=ColumnValidator(),
            )

            modified_pl_df = pl_df.with_columns([sample_col]).collect()
            converted_value = set(modified_pl_df[random_column_alias])

            self.assertEqual(len(converted_value), 1)
            self.assertEqual(list(converted_value)[0], date(year=2020, month=1, day=31))
