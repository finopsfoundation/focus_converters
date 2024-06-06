from typing import List, Tuple

import polars as pl

from focus_converter.configs.base_config import (
    ConversionPlan,
    MissingColumnDType,
    SetColumnDTypesConversionArgs,
)
from focus_converter.conversion_functions.validations import ColumnValidator


class DeferredColumnFunctions:
    """
    # Set of functions that can only be executed once the lazyframe is loaded and the column names available
    # are known.
    """

    def __init__(self):
        # missing_column_plans = []
        self.__missing_column_plans__: List[Tuple[str, ConversionPlan]] = []

        # enforced column dtypes, need to be applied before any other conversion
        # if column is present then a cast operation can be applied, if not then a new column can be added
        # with null values and then cast operation can be applied
        self.__enforced_column_dtypes__: List[ConversionPlan] = []

    @staticmethod
    def convert_focus_data_type_polars_dtype(focus_data_type):
        if focus_data_type == "string":
            return pl.Utf8
        elif focus_data_type == "float":
            return pl.Float64
        elif focus_data_type == "int":
            return pl.Int64
        elif focus_data_type == "datetime":
            return pl.Datetime
        elif focus_data_type == "date":
            return pl.Date
        else:
            raise RuntimeError(f"data_type: {focus_data_type} not implemented")

    def map_missing_column_plan(
        self, plan: ConversionPlan, column_alias, column_validator: ColumnValidator
    ):
        self.__missing_column_plans__.append((column_alias, plan))
        column_validator.map_static_default_value_if_not_present(
            plan=plan, column_alias=column_alias
        )

    def map_dtype_plan(self, plan: ConversionPlan, column_validator: ColumnValidator):
        self.__enforced_column_dtypes__.append(plan)
        column_validator.map_dtype_enforced_node(plan=plan)

    def apply_missing_column_plan(self, lf: pl.LazyFrame):
        for column_alias, missing_column_plan in self.__missing_column_plans__:
            if missing_column_plan.column not in lf.columns:
                conversion_arg: MissingColumnDType = MissingColumnDType.model_validate(
                    missing_column_plan.conversion_args
                )

                if conversion_arg.data_type == "string":
                    dtype = pl.Utf8
                elif conversion_arg.data_type == "float":
                    dtype = pl.Float64
                elif conversion_arg.data_type == "int":
                    dtype = pl.Int64
                else:
                    raise RuntimeError(
                        f"data_type: {conversion_arg.data_types} not implemented"
                    )

                lf = lf.with_columns(
                    pl.lit(None).cast(dtype).alias(missing_column_plan.column)
                )
            else:
                lf = lf.with_columns(
                    pl.col(missing_column_plan.column).alias(missing_column_plan.column)
                )
        return lf

    def apply_dtype_plan(self, lf: pl.LazyFrame):
        for plan in self.__enforced_column_dtypes__:
            conversion_args = SetColumnDTypesConversionArgs.model_validate(
                plan.conversion_args
            )
            for column_obj in conversion_args.dtype_args:
                if column_obj.column_name not in lf.columns:
                    lf = lf.with_columns(
                        pl.lit(None)
                        .cast(
                            self.convert_focus_data_type_polars_dtype(column_obj.dtype),
                            strict=False,
                        )
                        .alias(column_obj.column_name)
                    )
                else:
                    # check if the column is i64 for the cast to work else fail, at the point the column
                    # should be used with datetime parser plan.
                    cast_type = self.convert_focus_data_type_polars_dtype(
                        column_obj.dtype
                    )
                    if cast_type in [pl.Datetime, pl.Date]:
                        if lf.schema[column_obj.column_name] in [pl.Datetime, pl.Date]:
                            # ignore if the column is already of type datetime/date
                            pass
                        elif lf.schema[column_obj.column_name] == pl.Utf8:
                            if cast_type == pl.Datetime:
                                lf = lf.with_columns(
                                    pl.col(column_obj.column_name).str.to_datetime()
                                )
                            else:
                                lf = lf.with_columns(
                                    pl.col(column_obj.column_name).str.to_date()
                                )
                        else:
                            # possibly a timestamp column
                            lf = lf.with_columns(
                                pl.col(column_obj.column_name).cast(
                                    self.convert_focus_data_type_polars_dtype(
                                        column_obj.dtype
                                    ),
                                    strict=True,
                                )
                            )
                    else:
                        lf = lf.with_columns(
                            pl.col(column_obj.column_name).cast(
                                self.convert_focus_data_type_polars_dtype(
                                    column_obj.dtype
                                ),
                                strict=False,
                            )
                        )
        return lf
