---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Load a sample cost dataset as provider
2. Run conversion
3. If runs but produces incorrect result, run sql query on dataset using
```
import polars as pl

lf = pl.scan_parquet(<path_to_converted_data>)
sql_context = SQLFunctions.create_sql_context(lf=lf)
df = sql_context.execute(sql_query, eager=False).collect()
print(df.select(["column1", ...]).limit(10)
```

**Expected behavior**
Value format/value type.

**Screenshots**
If applicable, add screenshots to help explain your problem.

**Desktop (please complete the following information):**
 - OS: [e.g. iOS]
 - Converter version [e.g. 0.7]
 - Focus Spec Version [e.g. 1.0]

**Additional context**
Add any other context about the problem here.
