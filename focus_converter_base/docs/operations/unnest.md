# Unnesting Configuration with Custom Arguments

This document explains how to extend the YAML configuration for unnesting data, particularly when dealing with complex
nested structures like lists or structs. We'll utilize the `UnnestValueConversionArgs` model to specify behavior during
the unnesting process.

## Model Explanation

`UnnestValueConversionArgs` is a model used to control how unnesting behaves with respect to the nested data. Below are
the attributes:

- `children_type`: Specifies the type of the nested data. The data can either be a list of structures or a single
  structure. Defaults to "struct."

- `aggregation_operation`: Determines how to aggregate values if there are multiple possibilities due to the unnesting.
  By default, it takes the "first" value. Other options include "last," "sum," "mean," "min," or "max."

## Configuration Steps

1. **Prepare Your YAML File**: Create a new `.yaml` file or use an existing one where the unnest operation is defined.

2. **Specify Conversion Arguments**: After defining the basic unnesting parameters (`plan_name`, `conversion_type`,
   etc.), you'll need to add the `conversion_args` with specifics from the `UnnestValueConversionArgs` model. Ensure you
   maintain the correct indentation.

Here’s how your configuration might look:

```yaml
plan_name: convert location.location to tmp_Location for XORing Region/Location
conversion_type: unnest
column: location.location
focus_column: Region
column_prefix: tmp_location
conversion_args:
    children_type: list  # or "struct" depending on your data
    aggregation_operation: first  # can be "last", "sum", "mean", "min", "max"
```

3. **Validate Your Configuration**: After setting up the YAML file, validate it to ensure there are no syntax or logical
   errors. Use built-in validators in your environment or online tools.

4. **Apply the Configuration**: Save your changes and apply the configuration according to your application or system's
   procedures.

## Expected Behavior

- The `children_type` will inform the system of the nature of the nested data, allowing it to correctly process either
  lists or individual structs.
- The `aggregation_operation` will guide the system on how to handle multiple values emerging from a single unnested
  field. For example, if it's set to "sum," the system will add up all the values from the unnested data; if it's "
  first," it will select the first encountered value, and so on.

## Troubleshooting

- **Incorrect Unnesting Results**: If the data isn't unnesting as expected, review the `children_type`
  and `aggregation_operation` values. Ensure they're correctly set based on your data's structure and your intended
  outcome.
- **Errors or Exceptions**: Any syntax errors in the YAML or logical issues with the configuration can cause failures.
  Double-check your configuration, validate it, and consult application logs for detailed error messages.
