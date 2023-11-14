from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames
from tests.plan_validators.fixtures import *


def test_disconnected_nodes(
    sample_provider_name,
    sample_dimension_id,
    sample_step_id,
    sample_config_file_name,
    sample_column_name,
):
    """
    Tests that disconnected nodes are not allowed in the graph
    sql plans might create an intermediate column that is not used in the final output
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
        conversion_args=f"SELECT price, quantity, price * quantity AS total FROM sales;",
    )

    focus_converter = FocusConverter(column_prefix=None)
    focus_converter.plans = {sample_provider_name: [test_plan]}

    with pytest.raises(ValueError) as cm:
        focus_converter.prepare_horizontal_conversion_plan(
            provider=sample_provider_name
        )

    assert (
        str(cm.value)
        == "('Following sink nodes are not connected, potentially missing transform steps', {'total'})"
    )
