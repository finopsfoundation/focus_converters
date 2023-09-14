import polars as pl
from jinja2 import Environment

from focus_converter.configs.base_config import (
    ConversionPlan,
    SQLConditionConversionArgs,
)

# used in sql queries
# TODO: Make it configurable according to context if needed
DEFAULT_SQL_TABLE_NAME = "cost_data"


SQL_TEMPLATE_CONDITION_PLAN = """
select *, case {{ CASE_CONDITIONS }} ELSE {{ DEFAULT_VALUE }} END as {{ NEW_COLUMN }} from {{ TABLE_NAME }}
"""


class SQLFunctions:
    @staticmethod
    def create_sql_context(lf: pl.LazyFrame):
        sql_context = pl.SQLContext()
        sql_context.register(DEFAULT_SQL_TABLE_NAME, lf)
        return sql_context

    @staticmethod
    def eval_sql_conditions(plan: ConversionPlan, column_alias):
        conversion_args = SQLConditionConversionArgs.model_validate(
            plan.conversion_args
        )

        case_statements = []
        for condition in conversion_args.conditions:
            case_statements.append(condition)

        template = Environment().from_string(SQL_TEMPLATE_CONDITION_PLAN)
        sql_query = template.render(
            CASE_CONDITIONS=",".join(case_statements),
            DEFAULT_VALUE=conversion_args.default_value,
            NEW_COLUMN=column_alias,
            TABLE_NAME=DEFAULT_SQL_TABLE_NAME,
        )
        return sql_query

    @staticmethod
    def eval_sql_query(plan: ConversionPlan, column_alias):
        template = Environment().from_string(plan.conversion_args)
        sql_query = template.render(
            TABLE_NAME=DEFAULT_SQL_TABLE_NAME,
        )
        return sql_query
