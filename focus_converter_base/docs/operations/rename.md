# Renaming Column Configuration Guide

This document provides instructions on creating a YAML configuration to rename a column in your dataset. In the example,
we'll be changing the column name from `billing_account_id` to `BillingAccountId`.

## Configuration Details

This configuration is meant to perform a simple renaming of a column. Here’s what each key-value pair in the YAML
configuration represents:

- `plan_name`: This is a descriptive name for the operation or plan to be executed. In this case, it is "convert
  billing_account_id to BillingAccountID."
- `conversion_type`: This specifies the kind of operation to be performed on the data. Here, we're using "rename_column"
  to indicate a column name change.
- `column`: This represents the current name of the column in the dataset, which is to be renamed. In this context, it
  is "billing_account_id."
- `focus_column`: This specifies the new name that will replace the old column name after the operation is executed.
  Here, it will be "BillingAccountId."

Sample:

```yaml
    plan_name: convert billing_account_id to BillingAccountID
    conversion_type: rename_column
    column: billing_account_id
    focus_column: BillingAccountId
```
