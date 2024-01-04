import polars as pl
import sqlglot
from jinja2 import Environment

from focus_converter.configs.base_config import (
    ConversionPlan,
    SQLConditionConversionArgs,
)
from focus_converter.conversion_functions.validations import ColumnValidator

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
    def __validate_sql_query__(sql_query):
        try:
            sqlglot.transpile(sql_query)
        except sqlglot.errors.ParseError:
            raise ValueError(f"Invalid sql produced: {sql_query}")

    @classmethod
    def eval_sql_conditions(
        cls, plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ):
        conversion_args = SQLConditionConversionArgs.model_validate(
            plan.conversion_args
        )

        case_statements = []
        for condition in conversion_args.conditions:
            case_statements.append(condition)

        template = Environment().from_string(SQL_TEMPLATE_CONDITION_PLAN)
        sql_query = template.render(
            CASE_CONDITIONS=" ".join(case_statements),
            DEFAULT_VALUE=conversion_args.default_value,
            NEW_COLUMN=column_alias,
            TABLE_NAME=DEFAULT_SQL_TABLE_NAME,
        )

        # validate sql query structure
        cls.__validate_sql_query__(sql_query=sql_query)

        # destructure sql query and validate column names
        column_validator.map_sql_query(sql_query=sql_query, plan=plan)

        return sql_query

    @classmethod
    def eval_sql_query(
        cls, plan: ConversionPlan, column_validator: ColumnValidator, **_kwargs
    ):
        template = Environment().from_string(plan.conversion_args)
        sql_query = template.render(
            TABLE_NAME=DEFAULT_SQL_TABLE_NAME,
        )

        # validate sql query structure
        cls.__validate_sql_query__(sql_query=sql_query)

        # destructure sql query and validate column names
        column_validator.map_sql_query(sql_query=sql_query, plan=plan)

        return sql_query
