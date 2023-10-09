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
