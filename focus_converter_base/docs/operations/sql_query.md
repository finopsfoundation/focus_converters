# SQL Query-Based Column Computation Guide

This document outlines the steps to create a YAML configuration for computing a new column in your dataset using a SQL
query. Specifically, this guide focuses on computing the `BilledCost` by adding the `cost`
and `tmp_credits_amount_BilledCost` columns.

## Configuration Details

This configuration uses a SQL query to calculate the value of a new or existing column in your data. Here’s what each
key in the YAML configuration represents:

- `plan_name`: This is a descriptive name for the operation, which is "compute billed_cost" in this case.
- `conversion_type`: This specifies the type of operation to be performed on the data. Here, it is "sql_query,"
  indicating that a SQL query will be used for the conversion.
- `column`: This represents the column used in the computation, which is `tmp_credits_amount_BilledCost`.
- `focus_column`: This is the target column, which will hold the calculated value after the operation, `BilledCost` in
  this case.
- `conversion_args`: This contains the SQL query that performs the calculation. The `{{ TABLE_NAME }}` is a placeholder
  that will be replaced with the actual table name during execution.

sample:

```yaml
    plan_name: compute billed_cost
    conversion_type: sql_query
    column: tmp_credits_amount_BilledCost
    focus_column: BilledCost
    conversion_args: SELECT *, (cost + tmp_credits_amount_BilledCost) AS BilledCost from {{ TABLE_NAME }}
```
