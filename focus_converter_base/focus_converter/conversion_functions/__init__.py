from enum import Enum


class STATIC_CONVERSION_TYPES(Enum):
    # datetime functions
    CONVERT_TIMEZONE = "convert_timezone"
    ASSIGN_TIMEZONE = "assign_timezone"
    ASSIGN_UTC_TIMEZONE = "assign_utc_timezone"
    PARSE_DATETIME = "parse_datetime"

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

    # allows setting static NULL literal where values are null as defined in FOCUS spec
    CHANGE_NULL_VALUES_TO_LITERAL_NULL = "apply_null_literal"


__all__ = [
    "STATIC_CONVERSION_TYPES",
]
