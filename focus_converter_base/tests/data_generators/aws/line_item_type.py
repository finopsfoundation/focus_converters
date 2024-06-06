from enum import Enum


class LineItemType(Enum):
    SAVINGS_PLAN_RECURRING_FEE = "SavingsPlanRecurringFee"
    SAVINGS_PLAN_NEGATION = "SavingsPlanNegation"
    DISCONTINUED_USAGE = "DiscountedUsage"
    RI_FEE = "RIFee"
    USAGE = "Usage"
    SAVINGS_Plan_CCOVERED_USAGE = "SavingsPlanCoveredUsage"
