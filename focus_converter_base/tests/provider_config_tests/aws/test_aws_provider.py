from unittest import TestCase

import pandas as pd
import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.converter import FocusConverter


class TestAWSProvider(TestCase):
    def test_aws_provider_config(self):
        conversion_plan = ConversionPlan.load_yaml(
            "focus_converter/conversion_configs/aws/provider_S001.yaml"
        )

        focus_converter = FocusConverter()
        focus_converter.plans = {"aws": [conversion_plan]}
        column_exprs = focus_converter.prepare_horizontal_conversion_plan(
            provider="aws"
        )

        test_dataframe = pd.DataFrame([{"a": 1}])

        test_pl_df = (
            pl.from_dataframe(test_dataframe)
            .lazy()
            .with_columns(column_exprs)
            .collect()
        )
        self.assertEqual(list(test_pl_df["Provider"])[0], "AWS")
