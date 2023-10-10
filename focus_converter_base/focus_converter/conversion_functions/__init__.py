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


__all__ = [
    "STATIC_CONVERSION_TYPES",
]
