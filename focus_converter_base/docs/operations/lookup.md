# ServiceCategory Configuration for AWS Data

This guide explains how to create a YAML configuration for generating the `ServiceCategory` for AWS data using a map.
This configuration leverages a lookup conversion type to map `line_item_product_code` from your AWS billing data to a
more general `ServiceCategory` based on a reference dataset.

## Configuration Breakdown

Here's an overview of each key-value pair in the YAML configuration:

- `plan_name`: The name of the execution plan. Here, it's set to "Generate ServiceCategory for AWS data using a map."
- `column`: Specifies the source column name in the original data, which is "line_item_product_code" in this case.
- `conversion_type`: Defines the type of data conversion to be performed. We're using "lookup" to map the values from
  the source column to a new column.
- `focus_column`: The name of the new column to be created, "ServiceCategory."
- `conversion_args`: Arguments specific to the conversion. This includes the path to the reference dataset and specifies
  which columns to use for mapping.

### `conversion_args` Details

- `reference_dataset_path`: The file path to the CSV file containing the mapping between AWS product codes and
  categories.
- `source_value`: The column name in the reference dataset that corresponds to the source column in your data, "
  product_code."
- `destination_value`: The column name in the reference dataset that contains the values to which the source values will
  map, "aws_category."
- `reference_path_in_package`: The file can be outside of this software package and can be imported
  without path verification.

## Steps to Create the Configuration

1. **Prepare the Reference Dataset**: Ensure "aws_catergory_mapping.csv" is ready and accessible in the specified
   path (`"conversion_configs/aws/mapping_files/aws_catergory_mapping.csv"`). This file should contain at least two
   columns: one for the AWS product code (`product_code`) and one for the corresponding AWS service
   category (`aws_category`).

```yaml
plan_name: Generate ServiceCategory for AWS data using a map
column: line_item_product_code
conversion_type: lookup
focus_column: ServiceCategory
conversion_args:
    reference_dataset_path: "conversion_configs/aws/mapping_files/aws_catergory_mapping.csv"
    source_value: product_code
    destination_value: aws_category
```

## Troubleshooting

- **Invalid YAML Syntax**: YAML is sensitive to formatting and especially indentation. If you encounter syntax errors,
  double-check your indentations and ensure you're using spaces, not tabs.
- **File Not Found Errors**: If the system reports that it cannot find "aws_catergory_mapping.csv," verify that
  the `reference_dataset_path` is correct and the file is accessible.
- **Incorrect Mappings**: If the `ServiceCategory` values don't seem to match what's expected, ensure that
  the `source_value` and `destination_value` correctly correspond to the columns in "aws_catergory_mapping.csv".
