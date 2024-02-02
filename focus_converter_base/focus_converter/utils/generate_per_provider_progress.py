import os
from typing import Annotated

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import typer

app = typer.Typer(
    name="FOCUS progress pie chart generator per providers", add_completion=False
)


def autopct_format(values):
    def my_format(pct):
        total = sum(values)
        val = int(round(pct * total / 100.0))
        return "{:.1f}%\n({v:d})".format(pct, v=val)

    return my_format


@app.command("generate", help="Generates progress pie chart for a providers")
def generate_for_all_providers(
    output_dir: Annotated[
        str,
        typer.Option(help="Write provider csvs to this path."),
    ],
    csv_rules_export_dir: Annotated[
        str,
        typer.Option(
            help="Provider csvs rules export to generate progress pie charts."
        ),
    ],
):
    for i, provider_csv_rule_export_name in enumerate(os.listdir(csv_rules_export_dir)):
        provider = provider_csv_rule_export_name.split(".")[0]
        csv_path = os.path.join(csv_rules_export_dir, provider_csv_rule_export_name)

        rules = pl.read_csv(csv_path)
        rules = rules.group_by(["FOCUS Dimension"]).agg(
            pl.col("Transform Type").first()
        )

        total_rule_count = rules.shape[0]

        pending = rules.filter(pl.col("Transform Type").eq("Not Defined"))

        pending_count = pending.shape[0]
        done_count = total_rule_count - pending_count

        y = np.array([pending_count, done_count])
        labels = ["Pending", "Done"]

        fig, ax = plt.subplots()
        ax.pie(y, labels=labels, autopct=autopct_format([pending_count, done_count]))
        ax.legend()
        ax.set_title(f"{provider.upper()} Progress")
        fig.savefig(f"{output_dir}/{provider}_progress_pie_chart.png")


if __name__ == "__main__":
    app()
