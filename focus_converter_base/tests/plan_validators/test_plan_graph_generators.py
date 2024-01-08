import polars as pl

from focus_converter.configs.base_config import (
    ConversionPlan,
    LookupConversionArgs,
    SQLConditionConversionArgs,
    StaticValueConversionArgs,
)
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames
from tests.plan_validators.fixtures import *


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
    assert str(cm.value) == f"Column(s) '{sample_column_name}' not found in data"


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
            destination_value="line_item_product_code",
            reference_dataset_path="conversion_configs/aws/mapping_files/aws_category_mapping.csv",
            source_value="line_item_product_code",
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
    assert str(cm.value) == f"Column(s) '{sample_column_name}' not found in data"


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
    assert str(cm.value) == f"Column(s) '{sample_column_name}' not found in data"


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
    assert str(cm.value) == f"Column(s) '{sample_column_name}' not found in data"


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
    expected_output = f"{UML_GRAPH_HEADER}\n"
    expected_output += f"\tSOURCE --> {sample_column_name}\n"
    expected_output += f"\t{sample_column_name} -- ASSIGN_UTC_TIMEZONE:{sample_config_file_name} --> Provider\n"
    expected_output += "\tProvider --> FOCUS_DATASET\n"

    assert uml == expected_output

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column(s) '{sample_column_name}' not found in data"


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
    expected_output = f"{UML_GRAPH_HEADER}\n"
    expected_output += f"\tSOURCE --> {sample_column_name}\n"
    expected_output += f"\t{sample_column_name} -- PARSE_DATETIME:{sample_config_file_name} --> Provider\n"
    expected_output += "\tProvider --> FOCUS_DATASET\n"

    assert uml == expected_output

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == f"Column(s) '{sample_column_name}' not found in data"


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
        conversion_args=f"SELECT price, quantity, price * quantity AS Provider FROM sales;",
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    expected_output = f"{UML_GRAPH_HEADER}\n"
    expected_output += "\tSOURCE --> price\n"  # picks price from the source dataset
    expected_output += (
        "\tSOURCE --> quantity\n"  # picks quantity from the source dataset
    )
    expected_output += f"\tprice -- SQL_QUERY:{sample_config_file_name} --> Provider\n"  # maps price to total
    expected_output += f"\tquantity -- SQL_QUERY:{sample_config_file_name} --> Provider\n"  # maps quantity to total
    expected_output += "\tProvider --> FOCUS_DATASET\n"

    assert uml == expected_output

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == "Column(s) 'price, quantity' not found in data"


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

    sample_column_name = "column_" + sample_column_name.replace("-", "_")

    test_plan = ConversionPlan(
        focus_column=FocusColumnNames.PROVIDER,
        column=sample_column_name,
        conversion_type=STATIC_CONVERSION_TYPES.SQL_CONDITION,
        config_file_name=sample_config_file_name,
        plan_name="Test Plan",
        priority=sample_step_id,
        dimension_id=sample_dimension_id,
        conversion_args=SQLConditionConversionArgs(
            conditions=["WHEN price > 100 THEN 1"], default_value="0"
        ),
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}
    focus_converter.prepare_horizontal_conversion_plan(provider=sample_provider_name)

    uml = focus_converter.__column_validator__.generate_mermaid_uml()
    expected_output = f"{UML_GRAPH_HEADER}\n"
    expected_output += "\tSOURCE --> price\n"  # picks price from the source dataset
    expected_output += f"\tprice -- SQL_CONDITION:{sample_config_file_name} --> Provider\n"  # maps price to total
    expected_output += "\tProvider --> FOCUS_DATASET\n"

    assert uml == expected_output

    # ensures exception is raised if source column is not found in the empty dataframe
    with pytest.raises(ValueError) as cm:
        focus_converter.__process_lazy_frame__(lf=pl.LazyFrame())
    assert str(cm.value) == "Column(s) 'price' not found in data"
