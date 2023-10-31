## Reference configs for writing configs.

### Requirements

- Basic knowledge of YAML syntax.
- Access to the system or application that uses this configuration.
- The reference dataset "aws_catergory_mapping.csv" should be available at the specified path.

#### Operation documentation examples

##### Column functions

1. Lookup, reference AWS category mappings. [AWS Service Category](operations/lookup.md)
2. [Unnest](operations/unnest.md)
3. [Static value assignment](operations/static_value.md)
4. [Map](operations/map.md)
5. [Rename](operations/rename.md)

#### Datetime functions

1. [Set date as UTC](operations/utc_assignment.md)
2. [Timezone assignments](operations/assign_timezone.md)
3. [Datetime parsing from string](operations/datetime_parsing.md)

##### SQL functions

1. [Condition statements](operations/sql_condition.md)
2. [SQL queries for more complicated scenarios](operations/sql_query.md)

#### Sample notebook showing the workflow of how to write conversion configs.

Notebook [writing_config.ipnyb](notebooks/writing_configs.ipynb) 
