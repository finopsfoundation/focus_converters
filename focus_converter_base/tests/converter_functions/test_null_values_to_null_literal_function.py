from unittest import TestCase
from uuid import uuid4

import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames


class TestNullValueToLiteral(TestCase):
    def test_with_null_value(self):
        plan_config = """
        plan_name: sample
        priority: 1
        column: aws_product_code
        conversion_type: apply_null_literal
        focus_column: Region
        """

        test_plan = ConversionPlan(
            plan_name="sample",
            priority=1,
            dimension_id=1,
            column="aws_product_code",
            conversion_type=STATIC_CONVERSION_TYPES.CHANGE_NULL_VALUES_TO_LITERAL_NULL,
            focus_column=FocusColumnNames.PROVIDER,
            config_file_name="D0001-S000.yaml",
        )
        sample_provider_name = str(uuid4())

        focus_converter = FocusConverter(column_prefix=None)
        focus_converter.plans = {sample_provider_name: [test_plan]}
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )

        lf = pl.DataFrame({"aws_product_code": [None, "123"]}).lazy()
        lf = focus_converter.__process_lazy_frame__(lf=lf)
        df = lf.collect()

        self.assertEqual(df["Provider"][0], "NULL")
        self.assertEqual(df["Provider"][1], "123")
