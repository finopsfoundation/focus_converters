from datetime import date, timedelta
from random import randint, random
from uuid import uuid4

from faker import Faker

GCP_COLUMNS = [
    "invoice",
    "billing_account_id",
    "location",
    "seller_name",
    "credits",
    "cost",
]


class GCPSampleDataGenerator:
    def __init__(self, num_rows: int, destination_path: str):
        self.__num_rows__ = num_rows
        self.__fake__ = Faker()
        self.__destination_path__ = destination_path

    def generate_row(self, *_args):
        row_data = {}

        for column in GCP_COLUMNS:
            match column:
                case "invoice":
                    # generate sample month
                    sample_month: date = self.__fake__.date_object()
                    month_string = sample_month.strftime("%Y%m")
                    row_data[column] = {"month": month_string}

                    # generate a sample charge period string between current and next month
                    charge_period_string = self.__fake__.date_time_between_dates(
                        datetime_start=sample_month,
                        datetime_end=(sample_month + timedelta(days=32)).replace(day=1),
                    )
                    row_data["usage_start_time"] = charge_period_string
                    row_data["usage_end_time"] = charge_period_string + timedelta(
                        hours=1
                    )
                case "location":
                    # to simulate region logic when location is null
                    location_obj = {}

                    region_or_location_selector = randint(0, 1)
                    if region_or_location_selector:
                        location_obj.update({"region": str(uuid4()), "location": None})
                    else:
                        location_obj.update({"region": None, "location": str(uuid4())})

                    row_data["location"] = location_obj
                case "seller_name":
                    # to simulate generate some values for seller for some rows

                    seller_name_selector = randint(0, 1)
                    if seller_name_selector:
                        row_data["seller_name"] = self.__fake__.company()
                    else:
                        row_data["seller_name"] = None
                case "credits":
                    row_data["credits"] = {"amount": random()}
                case "cost":
                    row_data["cost"] = random()
                case _:
                    row_data[column] = str(uuid4())

        return row_data
