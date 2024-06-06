import polars as pl

from focus_converter.configs.base_config import ConversionPlan, LookupConversionArgs
from focus_converter.conversion_functions.validations import ColumnValidator


class LookupFunction:
    @classmethod
    def map_values_using_lookup(
        cls, plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ):
        conversion_args = LookupConversionArgs.model_validate(plan.conversion_args)
        reference_data_lf = (
            pl.scan_csv(conversion_args.reference_dataset_path)
            .select([conversion_args.source_value, conversion_args.destination_value])
            .with_columns(
                [
                    pl.col(conversion_args.source_value).cast(pl.Utf8),
                ]
            )
            .rename({conversion_args.destination_value: column_alias})
        )

        # add to column validator and check if source column exists
        column_validator.map_non_sql_plan(plan=plan, column_alias=column_alias)

        return {
            "other": reference_data_lf,
            "left_on": plan.column,
            "how": "left",
            "right_on": conversion_args.source_value,
        }
