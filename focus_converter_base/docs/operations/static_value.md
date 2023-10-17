# Static Value Assignment Configuration Guide

This guide details the creation of a YAML configuration for assigning a static value to a new or existing column in your
dataset. Specifically, we are assigning the static value "AWS" to the "Provider" column.

## Configuration Details

This configuration is designed to assign a predefined, unchanging value to a column for every record in your dataset.
Below is a breakdown of the YAML configuration keys:

- `plan_name`: A descriptive name for the operation. In this scenario, it's "Assign provider."
- `conversion_type`: This dictates the kind of operation to be performed. Here, it is "static_value," which means a
  fixed value will be assigned.
- `conversion_args`: Specific arguments or parameters required for the conversion. In this case, it's the actual value
  to be assigned, which is "AWS."
- `column`: This is typically used to specify the existing column to be transformed. However, as we're assigning a
  static value potentially to a new column or a column that doesn't need to be referenced for this operation, it is
  marked as "NA" (Not Applicable).
- `focus_column`: The target column where the static value will be assigned, which is "Provider" in this case.

Sample:

```yaml
    plan_name: Assign provider
    conversion_type: static_value
    conversion_args:
        static_value: AWS
    column: NA
    focus_column: Provider
    ```
