from enum import Enum
from typing import Annotated

import pandas as pd
import typer
import yaml

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter
from focus_converter.models.focus_column_names import FocusColumnNames

app = typer.Typer(name="FOCUS export conversion rules", add_completion=False)


class ReportFormats(Enum):
    CSV = "csv"
    MARKDOWN = "markdown"


@app.command("export", help="Converts source cost data to FOCUS format")
def export_conversion_rules(
    output_dir: Annotated[
        str,
        typer.Option(help="Write provider csvs to this path."),
    ],
    output_format: Annotated[
        ReportFormats,
        typer.Option(help="Output format"),
    ],
):
    # iterate through providers
    converter = FocusConverter()
    converter.load_provider_conversion_configs()

    for provider, plans in converter.plans.items():
        rows = []
        converter.prepare_horizontal_conversion_plan(provider=provider)

        # count number of times a dimension is transformed
        transform_step_counter = {}

        source_column_dtypes = {}
        for plan in plans:
            if (
                plan.conversion_type
                == STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING
            ):
                source_column_dtypes[plan.column] = plan.conversion_args["data_type"]
            elif plan.conversion_type == STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES:
                for dtype_arg in plan.conversion_args["dtype_args"]:
                    source_column_dtypes[dtype_arg["column_name"]] = dtype_arg["dtype"]

        plan: ConversionPlan
        for i, plan in enumerate(plans):
            # skip if dtype plan
            if plan.conversion_type in [
                STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES,
                STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING,
            ]:
                continue

            try:
                transform_step_counter[plan.focus_column] += 1
            except KeyError:
                transform_step_counter[plan.focus_column] = 1

            if plan.conversion_type in [
                STATIC_CONVERSION_TYPES.SQL_CONDITION,
                STATIC_CONVERSION_TYPES.ASSIGN_STATIC_VALUE,
                STATIC_CONVERSION_TYPES.LOOKUP,
                STATIC_CONVERSION_TYPES.ASSIGN_STATIC_VALUE,
                STATIC_CONVERSION_TYPES.MAP_VALUES,
            ]:
                formatted_conversion_args = yaml.dump(plan.conversion_args)
            elif (
                plan.conversion_type
                == STATIC_CONVERSION_TYPES.APPLY_DEFAULT_IF_COLUMN_MISSING
            ):
                formatted_conversion_args = yaml.dump(plan.conversion_args["data_type"])
            else:
                formatted_conversion_args = plan.conversion_args

            rows.append(
                {
                    "FOCUS Dimension": plan.focus_column.value,
                    "Transform Step": transform_step_counter[plan.focus_column],
                    "Source Column": plan.column,
                    "Source Column Type": source_column_dtypes.get(
                        plan.column, "Not Defined"
                    ),
                    "Transform Type": plan.conversion_type.name,
                    "Filters/Process/Etc.": formatted_conversion_args,
                }
            )

        # add missing output columns plan
        for focus_column in FocusColumnNames:
            if (
                focus_column not in transform_step_counter
                and focus_column != FocusColumnNames.PLACE_HOLDER
            ):
                rows.append(
                    {
                        "FOCUS Dimension": focus_column.value,
                        "Transform Step": 0,
                        "Source Column": "Not Defined",
                        "Source Column Type": "Not Defined",
                        "Transform Type": "Not Defined",
                        "Filters/Process/Etc.": "Not Defined",
                    }
                )

        # sort rows by transform step and focus dimension
        rows = sorted(
            rows,
            key=lambda x: (x["Transform Step"] > 0, x["FOCUS Dimension"]),
        )

        df = pd.DataFrame(rows)
        if output_format == ReportFormats.CSV:
            df.to_csv(f"{output_dir}/{provider}.csv", index=False)
        elif output_format == ReportFormats.MARKDOWN:
            df.to_markdown(f"{output_dir}/{provider}.md", index=False)
        else:
            raise ValueError(f"Invalid output format: {output_format}")


if __name__ == "__main__":
    app()
