# Value Mapping Configuration Guide

This guide outlines the steps to create a YAML configuration for mapping specific values in a dataset to new ones using
predefined pairs of keys and values. This configuration is particularly useful for standardizing data, categorizing
values, or simplifying complex strings.

## Configuration Details

The keys for the YAML configuration are as follows:

- `plan_name`: A descriptive name for the operation. It helps identify the specific process or transformation being
  applied.
- `conversion_type`: This should be set to "map" to indicate that the operation involves a mapping of values.
- `conversion_args`: A complex argument that specifies the `value_list` for key-value pairs to map, a `default_value` to
  use if no matches are found, and a flag `apply_default_if_null` to decide if the default should be applied to null
  values.
- `column`: The name of the column in the dataset on which the mapping will be applied.
- `focus_column`: The name of the column where the result of the mapping will be stored.

Sample:

```yaml
    plan_name: Map specific column values
    conversion_type: map
    column: your_column_name_here
    focus_column: your_focus_column_here
    conversion_args:
        value_list:
            - key: "original_value_1"
              value: "new_value_1"
            - key: "original_value_2"
              value: "new_value_2"
            # Add more key-value pairs as required
        default_value: "default_value_here"
        apply_default_if_null: true # or false depending on your preference
    ```
