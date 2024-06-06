from abc import ABC, abstractmethod

from focus_converter.conversion_functions.column_functions import ColumnFunctions
from focus_converter.conversion_functions.datetime_functions import (
    DateTimeConversionFunctions,
)
from focus_converter.conversion_functions.lookup_function import LookupFunction
from focus_converter.conversion_functions.sql_functions import SQLFunctions
from focus_converter.conversion_functions.string_functions import StringFunctions


# Define a Command interface with a method called execute().
class Command(ABC):
    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def categorty(self):
        pass


# Date/Time Based Commands
class ConvertTimezoneCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            DateTimeConversionFunctions.convert_timezone(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "datetime"


class DateTimeConversionCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            DateTimeConversionFunctions.assign_timezone(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "datetime"


class DateTimeAssignUTCCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            DateTimeConversionFunctions.assign_utc_timezone(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "datetime"


class DateTimeMonthStartCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            DateTimeConversionFunctions.month_start(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "datetime"


class DateTimeMonthEndCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            DateTimeConversionFunctions.month_end(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "datetime"


class DateTimeParseDateTimeCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            DateTimeConversionFunctions.parse_datetime(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "datetime"


# Column Based Commands


class ColumnRenameCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            ColumnFunctions.rename_column_functions(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "column"


class ColumnUnnestCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            ColumnFunctions.unnest(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "column"


class ColumnMapValuesCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            ColumnFunctions.map_values(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "column"


class ColumnAssignStaticCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            ColumnFunctions.assign_static_value(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "column"


# SQL Based Commands


class SQLEvalQueryCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            SQLFunctions.eval_sql_query(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "sql"


class SQLEvalConditionsCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            SQLFunctions.eval_sql_conditions(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "sql"


# Lookup Based Commands
class LookupMapValuesCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        return column_exprs.append(
            LookupFunction.map_values_using_lookup(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "lookup"


# Deferred Column Based Commands
class DeferredColumnPlanApplyDefaultColumnCommand(Command):
    def execute(self, plan, column_alias, column_validator, deffered_column_function):
        return deffered_column_function.map_missing_column_plan(
            plan=plan,
            column_alias=column_alias,
            column_validator=column_validator,
        )

    def categorty(self):
        return "deferred"


class DeferredColumnMapDTypePlanCommand(Command):
    def execute(self, plan, column_alias, column_validator, deffered_column_function):
        return deffered_column_function.map_dtype_plan(
            plan=plan,
            column_validator=column_validator,
        )

    def categorty(self):
        return "deferred"


class StringFunctionsCommand(Command):
    def execute(self, plan, column_alias, column_validator, column_exprs):
        column_exprs.append(
            StringFunctions.convert(
                plan=plan,
                column_alias=column_alias,
                column_validator=column_validator,
            )
        )

    def categorty(self):
        return "string"
