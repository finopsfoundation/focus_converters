import polars as pl

from focus_converter.configs.base_config import (
    ConversionPlan,
    StaticValueConversionArgs,
    UnnestValueConversionArgs,
    ValueMapConversionArgs,
)
from focus_converter.conversion_functions.validations import (
    STATIC_VALUE_COLUMN,
    ColumnValidator,
)
from focus_converter.models.focus_column_names import FocusColumnNames


class ColumnFunctions:
    @staticmethod
    def rename_column_functions(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return pl.col(plan.column).alias(column_alias)

    @staticmethod
    def add_provider(provider, column_validator: ColumnValidator) -> pl.col:
        return pl.lit(provider).alias(FocusColumnNames.PROVIDER.value)

    @staticmethod
    def unnest(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        # validate conversion args
        if plan.conversion_args:
            conversion_args = UnnestValueConversionArgs.model_validate(
                plan.conversion_args
            )
        else:
            conversion_args = UnnestValueConversionArgs()

        # split column name by dots to find nested structs
        field_depths = plan.column.split(".")

        if conversion_args.children_type == "struct":
            predicate = pl.col(field_depths[0])
            for children in field_depths[1:]:
                predicate = predicate.struct.field(children)
        elif conversion_args.children_type == "list":
            # base predicate acts on each list element and collect child value
            # required base operation like min,max,sum etc can be applied on this base predicate.
            predicate = pl.col(field_depths[0]).list.eval(
                pl.element().struct.field(field_depths[1])
            )

            if conversion_args.aggregation_operation == "first":
                predicate = predicate.list.first()
            elif conversion_args.aggregation_operation == "last":
                predicate = predicate.list.last()
            elif conversion_args.aggregation_operation == "sum":
                predicate = predicate.list.sum()
            elif conversion_args.aggregation_operation == "mean":
                predicate = predicate.list.mean()
            elif conversion_args.aggregation_operation == "min":
                predicate = predicate.list.min()
            elif conversion_args.aggregation_operation == "max":
                predicate = predicate.list.max()
            else:
                raise RuntimeError(
                    f"Unknown aggregation_operation type: {conversion_args.aggregation_operation}"
                )
        else:
            raise RuntimeError(
                "Unknown children type: {}".format(conversion_args.children_type)
            )

        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(
            plan=plan, column_alias=column_alias, source_column=field_depths[0]
        )

        return predicate.alias(column_alias)

    @staticmethod
    def map_values(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ):
        # Converts values for a given dimension using a map. This map must also provide a default value.

        conversion_args = ValueMapConversionArgs.model_validate(plan.conversion_args)

        map_dict = {}
        for value_obj in conversion_args.value_list:
            map_dict.update({str(value_obj.key): value_obj.value})

        # if flag set, allow null value to be mapped to default value
        if conversion_args.apply_default_if_null:
            map_dict.update({None: conversion_args.default_value})
        else:
            map_dict.update({None: None})

        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return (
            pl.col(plan.column)
            .replace(map_dict, default=conversion_args.default_value)
            .alias(column_alias)
        )

    @staticmethod
    def assign_static_value(
        plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ) -> pl.col:
        conversion_args: StaticValueConversionArgs = (
            StaticValueConversionArgs.model_validate(plan.conversion_args)
        )

        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(
            plan=plan, column_alias=column_alias, source_column=STATIC_VALUE_COLUMN
        )

        return pl.lit(conversion_args.static_value).alias(column_alias)
