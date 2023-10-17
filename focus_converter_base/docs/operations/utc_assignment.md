# UTC Timezone Assignment Configuration Guide

This guide provides the steps to create a YAML configuration that assigns the UTC timezone to a datetime column in your
dataset.

## Configuration Details

This configuration is intended to convert a datetime column in your dataset to use the UTC timezone, ensuring that time
data is standardized and not affected by regional timezone differences. Here's a breakdown of the YAML configuration's
components:

- `plan_name`: Identifies the specific operation or transformation, which in this case is "convert
  bill_billing_period_end_date to BillingPeriodEnd."
- `conversion_type`: Specifies the type of operation, which is "assign_utc_timezone" for this configuration, indicating
  the assignment of the UTC timezone to the specified column's datetime data.
- `column`: The name of the existing column that is subject to the operation, `bill_billing_period_end_date` in this
  case.
- `focus_column`: The name of the target column that will store the processed data, `BillingPeriodEnd` here, indicating
  that the original column will be renamed in the process.

Sample:

```yaml
    plan_name: convert bill_billing_period_end_date to BillingPeriodEnd
    conversion_type: assign_utc_timezone
    column: bill_billing_period_end_date
    focus_column: BillingPeriodEnd
    ```
