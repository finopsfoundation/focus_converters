from enum import Enum

import polars as pl

DEFAULT_FOCUS_NAMESPACE = "F"


class FocusColumnNames(Enum):
    """
    Focus column names as described in https://focus.finops.org/#specification
    """

    PLACE_HOLDER = "PlaceHolder"

    CHARGE_PERIOD_START = "ChargePeriodStart"
    CHARGE_PERIOD_END = "ChargePeriodEnd"
    CHARGE_FREQUENCY = "ChargeFrequency"
    CHARGE_SUB_CATEGORY = "ChargeSubcategory"
    CHARGE_DESCRIPTION = "ChargeDescription"

    BILLING_PERIOD_START = "BillingPeriodStart"
    BILLING_PERIOD_END = "BillingPeriodEnd"

    PROVIDER = "Provider"
    PUBLISHER = "Publisher"
    INVOICE_ISSUER = "InvoiceIssuer"

    BILLING_ACCOUNT_ID = "BillingAccountId"
    BILLING_ACCOUNT_NAME = "BillingAccountName"

    BILLED_COST = "BilledCost"
    BILLED_CURRENCY = "BilledCurrency"
    BILLING_CURRENCY = "BillingCurrency"
    EFFECTIVE_COST = "EffectiveCost"

    REGION = "Region"

    CHARGE_TYPE = "ChargeType"

    SERVICE_CATEGORY = "ServiceCategory"
    SERVICE_NAME = "ServiceName"

    SUB_ACCOUNT_NAME = "SubAccountName"
    SUB_ACCOUNT_ID = "SubAccountId"

    AVAILABILITY_ZONE = "AvailabilityZone"

    RESOURCE_NAME = "ResourceName"
    RESOURCE_ID = "ResourceId"

    COMMITMENT_DISCOUNT_ID = "CommitmentDiscountId"
    COMMITMENT_DISCOUNT_TYPE = "CommitmentDiscountType"
    COMMITMENT_DISCOUNT_CATEGORY = "CommitmentDiscountCategory"

    PRICING_QUANTITY = "PricingQuantity"
    PRICING_UNIT = "PricingUnit"

    SKU_ID = "SkuId"
    SKU_PRICE_ID = "SkuPriceId"

    LIST_UNIT_PRICE = "ListUnitPrice"
    LIST_COST = "ListCost"

    USAGE_QUANTITY = "UsageQuantity"
    USAGE_UNIT = "UsageUnit"


FOCUS_DATETIME_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_dtype_for_focus_column_name(focus_column_name: FocusColumnNames):
    """
    Return the dtype for the focus column name
    """

    # convert loop to match statements
    match focus_column_name:
        case FocusColumnNames.PLACE_HOLDER.value:
            raise ValueError("PLACE_HOLDER is not a valid focus column name")
        case (
            FocusColumnNames.CHARGE_PERIOD_START
            | FocusColumnNames.CHARGE_PERIOD_END
            | FocusColumnNames.BILLING_PERIOD_START
            | FocusColumnNames.BILLING_PERIOD_END
        ):
            return pl.Datetime
        case (
            FocusColumnNames.AMORTISED_COST
            | FocusColumnNames.BILLED_COST
            | FocusColumnNames.EFFECTIVE_COST
            | FocusColumnNames.LIST_COST
            | FocusColumnNames.LIST_UNIT_PRICE
            | FocusColumnNames.PRICING_QUANTITY
            | FocusColumnNames.USAGE_QUANTITY
        ):
            return pl.Float64
        case _:
            return pl.Utf8
