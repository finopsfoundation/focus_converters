from random import randint
from uuid import uuid4

import polars as pl
import pytest

from focus_converter.configs.base_config import (
    ConversionPlan,
    StaticValueConversionArgs,
    LookupConversionArgs,
)
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames

UML_GRAPH_HEADER = "graph TD;"


@pytest.fixture(scope="function")
def sample_provider_name():
    yield str(uuid4())


@pytest.fixture(scope="function")
def sample_column_name():
    yield str(uuid4())


@pytest.fixture(scope="function")
def sample_dimension_id():
    yield randint(0, 100)


@pytest.fixture(scope="function")
def sample_step_id():
    yield randint(0, 100)


@pytest.fixture(scope="function")
def sample_config_file_name(sample_dimension_id, sample_step_id):
    dimension_name = (
        "D"
        + "".join(["0"] * (3 - len(str(sample_dimension_id))))
        + str(sample_dimension_id)
    )

    step_name = (
        "S" + "".join(["0"] * (3 - len(str(sample_step_id)))) + str(sample_step_id)
    )

    yield f"{dimension_name}_{step_name}.yaml"


def test_static_value_assignment_plan(
    sample_provider_name, sample_dimension_id, sample_step_id, sample_config_file_name
):
    """
    Test static value assignment plan, and make sure that source column is not required as it is a static value
    """

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column="NA",
        conversion_type=STATIC_CONVERSION_TYPES.ASSIGN_STATIC_VALUE,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
        conversion_args=StaticValueConversionArgs(static_value=sample_provider_name),
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "graph TD;\n\tSTATIC_VALUE -- ASSIGN_STATIC_VALUE:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            sample_config_file_name
        )
    )

    # ensures exception is not raised if source column is not found in the empty dataframe
    focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())


def test_unnest_assignment_plan(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test unnest assignment plan, and make sure that source column is required as it is a unnest
    """

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=f"{sample_column_name}.test_child_column",
        conversion_type=STATIC_CONVERSION_TYPES.UNNEST_COLUMN,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- UNNEST_COLUMN:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


def test_lookup_plan(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test lookup plan, and make sure that source column is required as it is a lookup
    """

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.LOOKUP,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
        conversion_args=LookupConversionArgs(
            destination_value="product_code",
            reference_dataset_path="conversion_configs/aws/mapping_files/aws_catergory_mapping.csv",
            source_value="product_code",
        ),
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- LOOKUP:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


# noinspection DuplicatedCode
def test_datetime_functions_convert_timezone(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test convert_timezone conversion function, and make sure that source column is required in the source dataset
    """

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.CONVERT_TIMEZONE,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
        conversion_args="UTC",
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- CONVERT_TIMEZONE:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


# noinspection DuplicatedCode
def test_datetime_functions_assign_timezone(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test assign_timezone conversion function, and make sure that source column is required in the source dataset
    """

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.ASSIGN_TIMEZONE,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
        conversion_args="UTC",
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- ASSIGN_TIMEZONE:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


def test_datetime_functions_assign_utc_timezone(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test assign_utc_timezone conversion function, and make sure that source column is required in the source dataset
    """
    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.ASSIGN_UTC_TIMEZONE,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- ASSIGN_UTC_TIMEZONE:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


# noinspection DuplicatedCode
def test_datetime_functions_parse_datetime(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test parse_datetime conversion function, and make sure that source column is required in the source dataset
    """

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.PARSE_DATETIME,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- PARSE_DATETIME:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


def test_sql_function_query(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test sql function query, and make sure that source column from queries is required in the source dataset
    """

    sample_column_name = "column_" + sample_column_name.replace("-", "_")

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.SQL_QUERY,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
        conversion_args=f"SELECT {sample_column_name} FROM FOCUS_DATASET as {FocusColumnNames.SERVICE_NAME.value}",
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    print(uml)
    assert (
        uml
        == "{}\n\tSOURCE --> {}\n\t{} -- PARSE_DATETIME:{} --> Provider\n\tProvider --> FOCUS_DATASET\n".format(
            UML_GRAPH_HEADER,
            sample_column_name,
            sample_column_name,
            sample_config_file_name,
        )
    )

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column {sample_column_name} not found in data"


def test_sql_function_condition(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Test sql function condition, and make sure that source column from queries is required in the source dataset
    """
