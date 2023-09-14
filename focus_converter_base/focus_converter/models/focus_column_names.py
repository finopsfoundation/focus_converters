from enum import Enum

DEFAULT_FOCUS_NAMESPACE = "F"


class FocusColumnNames(Enum):
    """
    Focus column names as described in https://focus.finops.org/#specification
    """

    CHARGE_PERIOD_START = "ChargePeriodStart"
    CHARGE_PERIOD_END = "ChargePeriodEnd"

    BILLING_PERIOD_START = "BillingPeriodStart"
    BILLING_PERIOD_END = "BillingPeriodEnd"

    PROVIDER = "Provider"
    PUBLISHER = "Publisher"

    BILLING_ACCOUNT_ID = "BillingAccountId"

    AMORTISED_COST = "AmortisedCost"
    BILLED_COST = "BilledCost"

    REGION = "Region"


FOCUS_DATETIME_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
