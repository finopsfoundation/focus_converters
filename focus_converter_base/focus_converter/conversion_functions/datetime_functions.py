import polars as pl

from focus_converter.configs.base_config import ConversionPlan


class DateTimeConversionFunctions:
    @staticmethod
    def convert_timezone(plan: ConversionPlan, column_alias) -> pl.col:
        return (
            pl.col(plan.column)
            .dt.convert_time_zone(plan.conversion_args)
            .alias(column_alias)
        )

    @staticmethod
    def assign_timezone(plan: ConversionPlan, column_alias) -> pl.col:
        return (
            pl.col(plan.column)
            .dt.replace_time_zone(plan.conversion_args, ambiguous="earliest")
            .alias(column_alias)
        )

    @staticmethod
    def assign_utc_timezone(plan: ConversionPlan, column_alias) -> pl.col:
        return pl.col(plan.column).dt.replace_time_zone("UTC").alias(column_alias)

    @staticmethod
    def parse_datetime(plan: ConversionPlan, column_alias) -> pl.col:
        return (
            pl.col(plan.column)
            .str.strptime(pl.Datetime, plan.conversion_args)
            .alias(column_alias)
        )
