# SQL Condition-Based Column Conversion Guide

This document provides instructions for creating a YAML configuration that applies a SQL condition to convert or derive
a column value in your dataset. Specifically, this configuration will establish `Region` based on the values
of `tmp-region-Region` or `tmp-location-Region` using a condition.

## Configuration Details

This configuration uses a SQL condition to determine the value of a new or existing column in your data. Here are the
specifics of the configuration keys:

- `plan_name`: A descriptive name for the operation, "convert tmp-region-Region to Region based on tmp-region-Region or
  tmp-location-Region" in this case.
- `conversion_type`: The type of operation, which is "sql_condition" here, indicating the use of a SQL-based conditional
  logic.
- `conversion_args`: This contains the specifics of the conditions to be evaluated. It includes the SQL condition and a
  default value if the condition doesn't hold.
- `column`: Specifies the name of the initial column being evaluated, which is `tmp_region_Region` in this case.
- `focus_column`: This is the target column, which will hold the final value after the condition has been
  processed, `Region` here.

The `conditions` within `conversion_args` is a list of SQL case statements without the CASE keyword. They are conditions
to be evaluated in order, and when a condition is met, the corresponding THEN part is executed.

Sample:

```yaml
    plan_name: convert tmp-region-Region to Region based on tmp-region-Region or tmp-location-Region
    conversion_type: sql_condition
    conversion_args:
        conditions:
            - WHEN tmp_region_Region is not null THEN tmp_region_Region
        default_value: tmp_location_Region
    column: tmp_region_Region
    focus_column: Region
   ```

## Expected Outcome

On implementation, this configuration will evaluate the condition specified for the `tmp_region_Region` column.
If `tmp_region_Region` is not null, its value will be assigned to the `Region` column. If this condition is not met (
i.e., if `tmp_region_Region` is null), the `Region` column's value will default to the value of `tmp_location_Region`.
