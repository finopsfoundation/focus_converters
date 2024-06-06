import importlib.resources
import re
from pathlib import Path
from typing import Any, List, Literal, Optional, Union

import pytz
import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    ValidationError,
    field_validator,
)
from pydantic_core.core_schema import ValidationInfo
from pytz.exceptions import UnknownTimeZoneError
from typing_extensions import Annotated

from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.models.focus_column_names import FocusColumnNames


class SQLConditionConversionArgs(BaseModel):
    conditions: List[str]
    default_value: Any


class LookupConversionArgs(BaseModel):
    reference_path_in_package: bool = True
    reference_dataset_path: FilePath
    source_value: str
    destination_value: str

    @field_validator("reference_dataset_path", mode="before")
    def __validate_reference_dataset_path__(
        cls, reference_dataset_path, field_info: ValidationInfo
    ):
        reference_path_in_package = field_info.data.get("reference_path_in_package")
        if reference_path_in_package:
            try:
                return (
                    importlib.resources.files("focus_converter")
                    .joinpath(reference_dataset_path)
                    .as_posix()
                )
            except AttributeError:
                # for older python versions wheres .files api is not available
                from pkg_resources import resource_filename

                return resource_filename("focus_converter", reference_dataset_path)
        else:
            return reference_dataset_path


class ValueMapItemConversionArgs(BaseModel):
    key: Union[str, int]
    value: str


class ValueMapConversionArgs(BaseModel):
    value_list: List[ValueMapItemConversionArgs]
    default_value: str

    # flag decides if default value should be applied on null values or leave it null if false
    apply_default_if_null: Optional[bool] = True


class StaticValueConversionArgs(BaseModel):
    static_value: Any


class UnnestValueConversionArgs(BaseModel):
    # child element type, could be a struct or a list of structs
    children_type: Literal["list", "struct"] = "struct"

    # default behaviour to return first value, since we don't know what is the datatype
    aggregation_operation: Optional[
        Literal["first", "last", "sum", "mean", "min", "max"]
    ] = "first"


class MissingColumnDType(BaseModel):
    data_type: Literal["string", "float", "int"]


class DTypeConversionArg(BaseModel):
    column_name: str
    dtype: Literal["string", "float", "int", "datetime", "date"]
    strict: bool = False


class SetColumnDTypesConversionArgs(BaseModel):
    dtype_args: List[DTypeConversionArg]


CONFIG_FILE_PATTERN = re.compile("(.+)_S[0-9]{3}.yaml")


class ConversionPlan(BaseModel):
    # config source file name
    config_file_name: str

    # a friendly name helpful for debugging purposes
    plan_name: str

    # id picked from the file name, starting with D.
    dimension_id: Union[str, int]

    # for some datasets we might have to do conversions in an order,
    # higher priority will guarantee that those plans are executed first.
    priority: int

    # column to apply this on
    # TODO: Add option to allow query from multiple columns
    column: str

    conversion_type: STATIC_CONVERSION_TYPES
    conversion_args: Annotated[Any, Field(validate_default=True)] = None

    # converted focus column name
    focus_column: FocusColumnNames

    # optional column fix useful for intermediate columns to be dropped from final dataset
    # this is optional, but if defined should have the suffix `tmp`
    column_prefix: Optional[str] = None

    @field_validator("conversion_args")
    @classmethod
    def conversion_args_validation(cls, v: Any, field_info: ValidationInfo) -> str:
        conversion_type: STATIC_CONVERSION_TYPES = field_info.data.get(
            "conversion_type"
        )

        if (
            conversion_type == STATIC_CONVERSION_TYPES.ASSIGN_TIMEZONE
            or conversion_type == STATIC_CONVERSION_TYPES.CONVERT_TIMEZONE
        ):
            try:
                pytz.timezone(v)
            except UnknownTimeZoneError:
                raise ValueError(
                    f"Invalid timezone specified in conversion_args for plan: {field_info.data}"
                )
        elif conversion_type == STATIC_CONVERSION_TYPES.SQL_CONDITION:
            try:
                SQLConditionConversionArgs.model_validate(v)
            except ValidationError:
                raise ValueError(
                    f"Invalid SQL condition specified in conversion_args for plan: {field_info.data}"
                )
        elif conversion_type == STATIC_CONVERSION_TYPES.LOOKUP:
            try:
                LookupConversionArgs.model_validate(v)
            except ValidationError as e:
                raise ValueError(
                    e,
                    f"Invalid lookup arg specified in conversion_args for plan: {field_info.data}",
                )
        elif conversion_type == STATIC_CONVERSION_TYPES.MAP_VALUES:
            try:
                ValueMapConversionArgs.model_validate(v)
            except ValidationError as e:
                raise ValueError(
                    e, f"Missing or bad mapping value argument: {field_info.data}"
                )
        elif conversion_type == STATIC_CONVERSION_TYPES.ASSIGN_STATIC_VALUE:
            try:
                StaticValueConversionArgs.model_validate(v)
            except ValidationError as e:
                raise ValueError(
                    e, f"Missing or bad static value argument: {field_info.data}"
                )
        elif conversion_type == STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING:
            try:
                MissingColumnDType.model_validate(v)
            except ValidationError as e:
                raise ValueError(
                    e, f"Missing or bad unnest value argument: {field_info.data}"
                )
        elif conversion_type == STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES:
            try:
                SetColumnDTypesConversionArgs.model_validate(v)
            except ValidationError as e:
                raise ValueError(
                    e, f"Missing or bad set column dtype argument: {field_info.data}"
                )
        return v

    @field_validator("column_prefix")
    def validate_focus_column(cls, v: Any):
        if isinstance(v, str) and not v.startswith("tmp_"):
            raise ValueError("column_prefix, if defined should have 'tmp_' prefix.")
        return v

    @staticmethod
    def load_yaml(config_path: FilePath):
        if isinstance(config_path, str):
            config_path = Path(config_path)

        with open(config_path) as fd:
            obj = yaml.safe_load(fd)
        assert (
            re.match(CONFIG_FILE_PATTERN, config_path.name) is not None
        ), f"Filename should match pattern: {CONFIG_FILE_PATTERN}"

        # extract priority of config from name starting with S in the pattern D000_S000.yaml
        dimension_id = re.search(r"(.+)_S\d+\.yaml", config_path.name).group(1)
        priority = re.search(r".+_S(\d+)\.yaml", config_path.name).group(1)
        obj["priority"] = int(priority)
        obj["dimension_id"] = dimension_id
        obj["config_file_name"] = config_path.name

        return ConversionPlan.model_validate(obj=obj)

    model_config = ConfigDict(extra="forbid")
