import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.configs.string_transform_args import (
    StringSplitArgument,
    StringTransformArgs,
)
from focus_converter.conversion_functions.validations import ColumnValidator


class StringFunctions:
    @staticmethod
    def convert(plan: ConversionPlan, column_alias, column_validator: ColumnValidator):
        conversion_args = StringTransformArgs.model_validate(plan.conversion_args)

        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        column_expr = pl.col(plan.column)

        for step in conversion_args.steps:
            if step == "lower":
                column_expr = column_expr.str.to_lowercase()
            elif step == "upper":
                column_expr = column_expr.str.to_uppercase()
            elif step == "title":
                column_expr = column_expr.str.to_titlecase()
            elif isinstance(step, StringSplitArgument):
                column_expr = column_expr.str.split(step.split_by)
                if step.index:
                    column_expr = column_expr.list.get(index=step.index)
            else:
                raise ValueError(f"Invalid step {step} in conversion plan {plan}")

        return column_expr.alias(column_alias)
