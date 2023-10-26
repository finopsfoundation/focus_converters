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
    INVOICE_ISSUER = "InvoiceIssuer"

    BILLING_ACCOUNT_ID = "BillingAccountId"
    BILLING_ACCOUNT_NAME = "BillingAccountName"

    AMORTISED_COST = "AmortisedCost"
    BILLED_COST = "BilledCost"
    BILLED_CURRENCY = "BilledCurrency"

    REGION = "Region"

    CHARGE_TYPE = "ChargeType"

    SERVICE_CATEGORY = "ServiceCategory"
    SERVICE_NAME = "ServiceName"

    SUB_ACCOUNT_NAME = "SubAccountName"
    SUB_ACCOUNT_ID = "SubAccountId"

    AVAILABILITY_ZONE = "AvailibityZone"

    RESOURCE_NAME = "ResourceName"
    RESOURCE_ID = "ResourceId"


FOCUS_DATETIME_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
