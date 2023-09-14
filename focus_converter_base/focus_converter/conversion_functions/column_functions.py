import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.models.focus_column_names import FocusColumnNames


class ColumnFunctions:
    @staticmethod
    def rename_column_functions(plan: ConversionPlan, column_alias) -> pl.col:
        return pl.col(plan.column).alias(column_alias)

    @staticmethod
    def add_provider(provider) -> pl.col:
        return pl.lit(provider).alias(FocusColumnNames.PROVIDER.value)

    @staticmethod
    def unnest(plan: ConversionPlan, column_alias) -> pl.col:
        # split column name by dots to find nested structs
        field_depths = plan.column.split(".")

        # start with first value as column predicate and add rest of the children as struct fields
        predicate = pl.col(field_depths[0])

        for children in field_depths[1:]:
            predicate = predicate.struct.field(children)

        return predicate.alias(column_alias)
