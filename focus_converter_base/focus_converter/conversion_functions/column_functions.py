import polars as pl

from focus_converter.configs.base_config import (
    ConversionPlan,
    ValueMapConversionArgs,
    StaticValueConversionArgs,
)
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
        default_struct_field_type = pl.Utf8
        if plan.conversion_args and plan.conversion_args.get("struct_field_type"):
            # if struct field type is configured, add it to the schema in case struct is not defined
            struct_field_type = plan.conversion_args.get("struct_field_type")
            if struct_field_type == "int":
                default_struct_field_type = pl.Int64
            elif struct_field_type == "float":
                default_struct_field_type = pl.Float64
            else:
                raise ValueError(f"Invalid struct field type: {struct_field_type}")

        # split column name by dots to find nested structs
        field_depths = plan.column.split(".")

        # start with first value as column predicate and add rest of the children as struct fields
        predicate = pl.struct(
            field_depths[0], schema={field_depths[1]: default_struct_field_type}
        )

        for children in field_depths[1:]:
            predicate = predicate.struct.field(children)

        return predicate.alias(column_alias)

    @staticmethod
    def map_values(plan: ConversionPlan, column_alias):
        # Converts values for a given dimension using a map. This map must also provide a default value.

        conversion_args = ValueMapConversionArgs.model_validate(plan.conversion_args)

        map_dict = {}
        for value_obj in conversion_args.value_list:
            map_dict.update({value_obj.key: value_obj.value})

        # if flag set, allow null value to be mapped to default value
        if conversion_args.apply_default_if_null:
            map_dict.update({None: conversion_args.default_value})
        else:
            map_dict.update({None: None})

        return (
            pl.col(plan.column)
            .map_dict(map_dict, default=conversion_args.default_value)
            .alias(column_alias)
        )

    @staticmethod
    def assign_static_value(plan: ConversionPlan, column_alias) -> pl.col:
        conversion_args: StaticValueConversionArgs = (
            StaticValueConversionArgs.model_validate(plan.conversion_args)
        )
        return pl.lit(conversion_args.static_value).alias(column_alias)
