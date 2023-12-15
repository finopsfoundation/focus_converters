import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions.validations import ColumnValidator


class DateTimeConversionFunctions:
    @staticmethod
    def convert_timezone(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return (
            pl.col(plan.column)
            .dt.cast_time_unit("ms")
            .dt.convert_time_zone(plan.conversion_args)
            .alias(column_alias)
        )

    @staticmethod
    def assign_timezone(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return (
            pl.col(plan.column)
            .dt.cast_time_unit("ms")
            .dt.replace_time_zone(plan.conversion_args, ambiguous="earliest")
            .alias(column_alias)
        )

    @staticmethod
    def assign_utc_timezone(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return (
            pl.col(plan.column)
            .dt.cast_time_unit("ms")
            .dt.replace_time_zone("UTC")
            .alias(column_alias)
        )

    @staticmethod
    def parse_datetime(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return (
            pl.col(plan.column)
            .str.strptime(pl.Datetime, plan.conversion_args)
            .dt.cast_time_unit("ms")
            .alias(column_alias)
        )

    @staticmethod
    def month_start(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return pl.col(plan.column).dt.date().dt.month_start().alias(column_alias)

    @staticmethod
    def month_end(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return pl.col(plan.column).dt.date().dt.month_end().alias(column_alias)
