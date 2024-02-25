import importlib.resources
import logging
import os
from operator import attrgetter
from typing import Dict, List, Optional

import polars as pl

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.conversion_functions.deferred_column_functions import (
    DeferredColumnFunctions,
)
from focus_converter.conversion_functions.sql_functions import SQLFunctions
from focus_converter.conversion_functions.validations import ColumnValidator
from focus_converter.conversion_strategy import (
    ColumnAssignStaticCommand,
    ColumnMapValuesCommand,
    ColumnRenameCommand,
    ColumnUnnestCommand,
    ConvertTimezoneCommand,
    DateTimeAssignUTCCommand,
    DateTimeConversionCommand,
    DateTimeMonthEndCommand,
    DateTimeMonthStartCommand,
    DateTimeParseDateTimeCommand,
    DeferredColumnMapDTypePlanCommand,
    DeferredColumnPlanApplyDefaultColumnCommand,
    LookupMapValuesCommand,
    SQLEvalConditionsCommand,
    SQLEvalQueryCommand,
    StringFunctionsCommand,
)
from focus_converter.data_loaders.data_exporter import DataExporter
from focus_converter.data_loaders.data_loader import DataLoader
from focus_converter.models.focus_column_names import (
    FocusColumnNames,
    get_dtype_for_focus_column_name,
)

# TODO: Make this path configurable so that we can load from a directory outside of the project
BASE_CONVERSION_CONFIGS = (
    importlib.resources.files("focus_converter")
    .joinpath("conversion_configs")
    .as_posix()
)


class FocusConverter:
    plans: Dict[str, List[ConversionPlan]]
    data_loader: DataLoader
    data_exporter: DataExporter = None

    # set of plan variables for horizontal transformation plans
    h_collected_columns: List[str]  # collected columns
    h_column_exprs: List[pl.col]  # column expressions
    h_sql_queries: List[str]  # sql_queries

    # temporary columns to be removed from final dataset
    __temporary_columns__: List[str]

    # column prefix used in source dataset
    __column_prefix__: Optional[str] = None

    # converted column prefix to be added to converted columns
    __converted_column_prefix__: Optional[str] = None

    def __init__(self, column_prefix=None, converted_column_prefix=None):
        self.__temporary_columns__ = []
        self.__column_prefix__ = column_prefix
        self.__converted_column_prefix__ = converted_column_prefix

        # ColumnValidator, used to validate column names in sql queries and transformations
        self.__column_validator__ = ColumnValidator()
        self.plans = {}

        # deferred column plans, these plans are applied after lazyframe is loaded
        self.__deferred_column_plans__ = DeferredColumnFunctions()

    def load_provider_conversion_configs(self):
        plans = {}

        for provider in os.listdir(BASE_CONVERSION_CONFIGS):
            # collect all plans specific to vendor
            provider_plans: List[ConversionPlan] = []

            provider_base_path = os.path.join(BASE_CONVERSION_CONFIGS, provider)
            for provider_config_name in os.listdir(provider_base_path):
                if not provider_config_name.endswith(".yaml"):
                    # ignores non yaml files, which may be reference datasets
                    continue

                provider_config_path = os.path.join(
                    provider_base_path, provider_config_name
                )
                logging.debug(f"Reading plan from path: {provider_config_name}")
                provider_plans.append(ConversionPlan.load_yaml(provider_config_path))

            plans[provider] = sorted(
                provider_plans,
                key=attrgetter("dimension_id", "priority"),
                reverse=False,
            )
        self.plans = plans

    def load_data(self, *args, **kwargs):
        self.data_loader = DataLoader(*args, **kwargs)

    def configure_data_export(self, *args, **kwargs):
        self.data_exporter = DataExporter(*args, **kwargs)

    def prepare_horizontal_conversion_plan(self, provider):
        # final set of columns produced after this transform step
        self.h_collected_columns = collected_columns = []

        # column expressions that will be applied to loaded lazy frame in order
        self.h_column_exprs = column_exprs = []

        # sql queries collected to be applied on the lazy frame
        self.h_sql_queries = sql_queries = []

        # lookup lazyframes arguments to be assembled later on the final source lazyframe
        self.lookup_reference_args = []

        # Create a dictionary to map conversion types to command classes.
        command_classes = {
            # Column based commands
            STATIC_CONVERSION_TYPES.CONVERT_TIMEZONE: ConvertTimezoneCommand,
            STATIC_CONVERSION_TYPES.ASSIGN_TIMEZONE: DateTimeConversionCommand,
            STATIC_CONVERSION_TYPES.ASSIGN_UTC_TIMEZONE: DateTimeAssignUTCCommand,
            STATIC_CONVERSION_TYPES.MONTH_START: DateTimeMonthStartCommand,
            STATIC_CONVERSION_TYPES.MONTH_END: DateTimeMonthEndCommand,
            STATIC_CONVERSION_TYPES.RENAME_COLUMN: ColumnRenameCommand,
            STATIC_CONVERSION_TYPES.MAP_VALUES: ColumnMapValuesCommand,
            STATIC_CONVERSION_TYPES.ASSIGN_STATIC_VALUE: ColumnAssignStaticCommand,
            # SQL based commands
            STATIC_CONVERSION_TYPES.SQL_QUERY: SQLEvalQueryCommand,
            STATIC_CONVERSION_TYPES.SQL_CONDITION: SQLEvalConditionsCommand,
            STATIC_CONVERSION_TYPES.PARSE_DATETIME: DateTimeParseDateTimeCommand,
            STATIC_CONVERSION_TYPES.UNNEST_COLUMN: ColumnUnnestCommand,
            # Lookup based commands
            STATIC_CONVERSION_TYPES.LOOKUP: LookupMapValuesCommand,
            # Deferred column plans
            STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING: DeferredColumnPlanApplyDefaultColumnCommand,
            STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES: DeferredColumnMapDTypePlanCommand,
            # String based plans
            STATIC_CONVERSION_TYPES.STRING_FUNCTIONS: StringFunctionsCommand,
        }

        for plan in self.plans[provider]:
            # column name generated with temporary prefix
            column_alias = self.generate_column_name(plan)

            # add column to plan to collect these dimensions to be added in the computed dataframe
            if plan.focus_column != FocusColumnNames.PLACE_HOLDER:
                collected_columns.append(plan.focus_column.value)

            # process data based on conversion type
            command_class = command_classes.get(plan.conversion_type)
            if (
                command_class.categorty(self) == "column"
                or command_class.categorty(self) == "datetime"
            ):
                command_class().execute(
                    plan, column_alias, self.__column_validator__, column_exprs
                )
            elif command_class.categorty(self) == "sql":
                command_class().execute(
                    plan, column_alias, self.__column_validator__, sql_queries
                )
            elif command_class.categorty(self) == "lookup":
                command_class().execute(
                    plan,
                    column_alias,
                    self.__column_validator__,
                    self.lookup_reference_args,
                )
            elif command_class.categorty(self) == "deferred":
                command_class().execute(
                    plan,
                    column_alias,
                    self.__column_validator__,
                    self.__deferred_column_plans__,
                )
            elif command_class.categorty(self) == "string":
                command_class().execute(
                    plan,
                    column_alias,
                    self.__column_validator__,
                    column_exprs,
                )
            else:
                raise NotImplementedError(
                    f"Plan: {plan.conversion_type} not implemented"
                )

        # apply the plan to the lazy frame
        self.__column_validator__.validate_graph_is_connected()

        return column_exprs

    def generate_column_name(self, plan):
        if plan.column_prefix:
            column_alias = f"{plan.column_prefix}_{plan.focus_column.value}"
            self.__temporary_columns__.append(column_alias)
        else:
            column_alias = plan.focus_column.value
        return column_alias

    @staticmethod
    def __apply_sql_queries__(lf: pl.LazyFrame, sql_queries):
        for sql_query in sql_queries:
            sql_context = SQLFunctions.create_sql_context(lf=lf)
            lf = sql_context.execute(sql_query, eager=False)
        return lf

    @staticmethod
    def __apply_column_expressions__(
        lf: pl.LazyFrame, column_expressions: List[pl.col]
    ):
        for expr in column_expressions:
            lf = lf.with_columns_seq(expr)
        return lf

    @staticmethod
    def __apply_lookup_reference_plans__(lf: pl.LazyFrame, lookup_args):
        for lookup_arg in lookup_args:
            lf = lf.join(**lookup_arg)
        return lf

    def explain(self):
        # get batched data lazy frame, build the plan and then break
        return self.__column_validator__.generate_uml_graph()

    def apply_plan(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        # creates lazy frame using the config, actual computation happens in collect

        lf = self.__apply_column_expressions__(
            lf=lf, column_expressions=self.h_column_exprs
        )

        # apply lazyframe joins
        lf = self.__apply_lookup_reference_plans__(
            lf=lf, lookup_args=self.lookup_reference_args
        )

        lf = self.__apply_sql_queries__(lf=lf, sql_queries=self.h_sql_queries)

        # drop temporary columns from the final dataset
        lf = lf.drop(self.__temporary_columns__)

        return lf

    def __re_map_source_columns__(self, lf: pl.LazyFrame):
        # helper function to re-map prefixed source columns to source columns
        # so that conversion plans can be applied.

        column_prefix_length = len(self.__column_prefix__)
        for column in lf.columns:
            if column.startswith(self.__column_prefix__):
                orig_column_name = column[column_prefix_length:]
                lf = lf.with_columns_seq(pl.col(column).alias(orig_column_name))

                # add remapped column to temporary columns list so that it can be dropped later
                self.__temporary_columns__.append(orig_column_name)
        return lf

    def __add_empty_columns_for_missing_focus_columns__(self, lf: pl.LazyFrame):
        # add missing focus columns with null values to produce a valid parquet file
        for focus_column_name in FocusColumnNames:
            if (
                focus_column_name.value not in self.h_collected_columns
                and focus_column_name != FocusColumnNames.PLACE_HOLDER
            ):
                lf = lf.with_columns(
                    pl.lit(None)
                    .alias(focus_column_name.value)
                    .cast(get_dtype_for_focus_column_name(focus_column_name))
                )
        return lf

    def __process_lazy_frame__(self, lf: pl.LazyFrame):
        # prepares lazyframe for the operations to be applied on the lazy loaded polars dataframe
        if self.__column_prefix__ is not None:
            lf = self.__re_map_source_columns__(lf=lf)

        # apply deferred column plans
        lf = self.__deferred_column_plans__.apply_missing_column_plan(lf=lf)
        lf = self.__deferred_column_plans__.apply_dtype_plan(lf=lf)

        # validate all source columns exist in the lazy frame
        self.__column_validator__.validate_lazy_frame_columns(lf=lf)

        # add missing focus columns with null values to produce a valid parquet file
        lf = self.__add_empty_columns_for_missing_focus_columns__(lf=lf)

        return self.apply_plan(lf=lf)

    def convert(self):
        for lf in self.data_loader.data_scanner():
            lf = self.__process_lazy_frame__(lf=lf)
            self.data_exporter.collect(
                lf=lf, collected_columns=list(set(self.h_collected_columns))
            )
        self.data_exporter.close()
