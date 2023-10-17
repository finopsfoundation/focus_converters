# Datetime Parsing Configuration Guide

This document provides instructions on creating a YAML configuration to parse a datetime column in your dataset,
specifically to extract the year and month in the `YYYYmm` format from the `BillingPeriodStart` column.

## Configuration Details

This configuration aims to parse datetime values from a specific column and convert them into a desired string format.
Here’s what each key in the YAML configuration stands for:

- `plan_name`: A descriptive name for the operation or plan to be executed. In this case, it is "parse YYYYmm from
  BillingPeriodStart."
- `conversion_type`: This specifies the kind of operation to be performed on the data. Here, it is "parse_datetime,"
  which means the task involves parsing data from a datetime format.
- `column`: This represents the name of the existing column in the dataset, the subject of the operation, which
  is `BillingPeriodStart`.
- `focus_column`: This specifies the target column that will hold the processed data. In this context, it
  remains `BillingPeriodStart` as we are transforming the data in-place.
- `conversion_args`: This holds the specific argument for the conversion, the format code `"%Y%m"`, which instructs the
  system to parse the year and month from the datetime value.

Sample:

```yaml
    plan_name: parse YYYYmm from BillingPeriodStart
    conversion_type: parse_datetime
    column: BillingPeriodStart
    focus_column: BillingPeriodStart
    conversion_args: "%Y%m"
    ```
