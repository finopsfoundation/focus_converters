import re
from pathlib import Path
from typing import Any, List, Optional

import pytz
import yaml
from pydantic import (
    BaseModel,
    FilePath,
    ConfigDict,
    field_validator,
    ValidationError,
    Field,
)
from pydantic_core.core_schema import FieldValidationInfo
from pytz.exceptions import UnknownTimeZoneError
from typing_extensions import Annotated

from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.models.focus_column_names import FocusColumnNames


class SQLConditionConversionArgs(BaseModel):
    conditions: List[str]
    default_value: Any


CONFIG_FILE_PATTERN = re.compile("D\d{3}_S\d{3}.yaml")


class ConversionPlan(BaseModel):
    # a friendly name helpful for debugging purposes
    plan_name: str

    # id picked from the file name, starting with D.
    dimension_id: int

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
    def double(cls, v: Any, field_info: FieldValidationInfo) -> str:
        conversion_type: STATIC_CONVERSION_TYPES = field_info.data.get(
            "conversion_type"
        )
        match conversion_type:
            case (
                STATIC_CONVERSION_TYPES.ASSIGN_TIMEZONE
                | STATIC_CONVERSION_TYPES.CONVERT_TIMEZONE
            ):
                try:
                    pytz.timezone(v)
                except UnknownTimeZoneError:
                    raise ValueError(
                        f"Invalid timezone specified in conversion_args for plan: {field_info.data}"
                    )
            case STATIC_CONVERSION_TYPES.SQL_CONDITION:
                try:
                    SQLConditionConversionArgs.model_validate(v)
                except ValidationError:
                    raise ValueError(
                        f"Invalid SQL condition specified in conversion_args for plan: {field_info.data}"
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
        priority = config_path.name.split("_")[1][1:].replace(".yaml", "")
        dimension_id = config_path.name.split("_")[0].replace("D", "")
        obj["priority"] = int(priority)
        obj["dimension_id"] = int(dimension_id)

        return ConversionPlan.model_validate(obj=obj)

    model_config = ConfigDict(extra="forbid")
