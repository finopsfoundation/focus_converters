from enum import Enum


class STATIC_CONVERSION_TYPES(Enum):
    # datetime functions
    CONVERT_TIMEZONE = "convert_timezone"
    ASSIGN_TIMEZONE = "assign_timezone"
    ASSIGN_UTC_TIMEZONE = "assign_utc_timezone"
    PARSE_DATETIME = "parse_datetime"
    MONTH_START = "month_start"
    MONTH_END = "month_end"

    # sql rule functions
    SQL_QUERY = "sql_query"
    SQL_CONDITION = "sql_condition"

    # column rename function
    RENAME_COLUMN = "rename_column"

    # unnest operation
    UNNEST_COLUMN = "unnest"

    # lookup operation
    LOOKUP = "lookup"

    # value mapping function
    MAP_VALUES = "map_values"

    # allows setting static values
    ASSIGN_STATIC_VALUE = "static_value"

    # apply default values if column not present
    APPLY_DEFAULT_IF_COLUMN_MISSING = "apply_default_if_column_missing"

    # set column dtypes
    SET_COLUMN_DTYPES = "set_column_dtypes"

    # string functions
    STRING_FUNCTIONS = "string_functions"


__all__ = [
    "STATIC_CONVERSION_TYPES",
]
