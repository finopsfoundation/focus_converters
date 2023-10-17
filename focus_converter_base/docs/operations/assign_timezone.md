# Timezone Assignment Configuration Guide

This guide provides the steps to create a YAML configuration that assigns a specific timezone to a datetime column in
your dataset. In this instance, we're setting the timezone for the `BillingPeriodEnd` column to Pacific Standard Time (
PST), represented by the "America/Los_Angeles" timezone identifier.

## Configuration Details

This configuration aims to assign a specific timezone to a datetime column without altering the actual datetime. Here’s
a breakdown of the configuration keys:

- `plan_name`: Descriptive name of the operation. Here, it's "set BillingPeriodEnd as PST."
- `conversion_type`: Specifies the operation's type, which is "assign_timezone" in this case, indicating that a timezone
  will be set for the specified column.
- `column`: The name of the existing column that the operation will act upon, "BillingPeriodEnd" here.
- `focus_column`: The name of the target column that will reflect the changes. In this scenario, it remains "
  BillingPeriodEnd" since we're not creating a new column but modifying the existing one.
- `conversion_args`: The specific details required for the conversion. Here, it's the timezone identifier "
  America/Los_Angeles," which represents PST.

Sample:

```yaml
    plan_name: set BillingPeriodEnd as PST
    conversion_type: assign_timezone
    column: BillingPeriodEnd
    focus_column: BillingPeriodEnd
    conversion_args: America/Los_Angeles
    ```
