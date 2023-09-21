import logging
import multiprocessing

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import typer
from tqdm import tqdm

from tests.data_generators.aws.aws_sample_data_generator import \
    AWSSampleDataGenerator
from tests.data_generators.gcp.gcp_sample_data_generator import \
    GCPSampleDataGenerator

# default row group size for each fragment
ROW_GROUP_SIZE = 50000

# generated data disclaimer
DATA_DISCLAIMER = "Generated data is for performance and testing purposes only and may contain inaccurate data."

app = typer.Typer(add_completion=False)


@app.command("generate")
def generate(
    provider: str = typer.Option(help="Target provider"),
    num_rows: int = typer.Option(help="Number of rows to generate"),
    destination_path: str = typer.Option(help="Destination path to write dataframe"),
):
    logging.warning(DATA_DISCLAIMER)

    match provider:
        case "aws":
            generator = AWSSampleDataGenerator(
                num_rows=num_rows, destination_path=destination_path
            )
        case "gcp":
            generator = GCPSampleDataGenerator(
                num_rows=num_rows, destination_path=destination_path
            )
        case _:
            raise RuntimeError(f"Provider: {provider} not found.")

    # creates pool for generating sample data in processes and then writing it as one dataset
    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    for i in tqdm(range(0, num_rows, ROW_GROUP_SIZE)):
        limit = min(num_rows - i, ROW_GROUP_SIZE)

        generated_rows = pool.map(generator.generate_row, range(limit))
        arrow_table = pa.Table.from_pandas(pd.DataFrame(generated_rows))
        pq.write_to_dataset(table=arrow_table, root_path=destination_path)

    pool.close()


if __name__ == "__main__":
    app()
