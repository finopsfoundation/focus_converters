from uuid import uuid4

import faker.providers.python
from faker import Faker

from tests.data_generators.aws.columns import AWS_COLUMNS
from tests.data_generators.aws.line_item_type import LineItemType


class AWSSampleDataGenerator:
    """
    TODO: Add disclaimer this being performance/testing only.
    """

    def __init__(self, num_rows: int, destination_path: str):
        self.__num_rows__ = num_rows
        self.__fake__ = Faker()
        self.__add_line_item_provider__()
        self.__destination_path__ = destination_path

    def __add_line_item_provider__(self):
        line_item_type = faker.providers.DynamicProvider(
            provider_name="line_item_line_item_type", elements=list(LineItemType)
        )
        self.__fake__.add_provider(line_item_type)

    def generate_row(self, *_args):
        row_data = {}

        for column in AWS_COLUMNS:
            match column:
                case "line_item_line_item_type":
                    sample = self.__fake__.line_item_line_item_type()
                    row_data[column] = sample.value

                case (
                    "bill_billing_period_end_date"
                    | "bill_billing_period_start_date"
                    | "line_item_usage_start_date"
                    | "line_item_usage_end_date"
                ):
                    row_data[column] = self.__fake__.date_time()

                case (
                    "savings_plan_total_commitment_to_date"
                    | "savings_plan_used_commitment"
                    | "savings_plan_savings_plan_effective_cost"
                    | "reservation_effective_cost"
                    | "reservation_unused_amortized_upfront_fee_for_billing_period"
                    | "reservation_unused_recurring_fee"
                ):
                    row_data[column] = self.__fake__.pyfloat(min_value=0, max_value=100)

                case _:
                    row_data[column] = str(uuid4())

        return row_data
