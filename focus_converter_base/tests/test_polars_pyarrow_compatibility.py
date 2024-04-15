import os

import polars as pl
import pyarrow as pa
import pytest
from unittest import TestCase
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import tempfile

from focus_converter.data_loaders.data_loader import (
    DEFAULT_BATCH_READ_SIZE,
    FRAGMENT_READ_AHEAD,
    BATCH_READ_AHEAD,
)


class TestPolarsPyarrowCompatibility(TestCase):
    """
    Test that the Polars and PyArrow data types are compatible.

    When trying to load a PyArrow table into Polars, raises error AttributeError: 'pyarrow.lib.StructArray' object has no attribute 'num_chunks'
    """

    def test_polars_pyarrow_compatibility(self):
        # Create a PyArrow table
        table = pa.table(
            {"a": [1, 2, 3], "b": [4, 5, 6], "c": [{"d": 7}, {"d": 8}, {"d": 9}]}
        )

        with tempfile.TemporaryDirectory() as tempdir:
            pq.write_table(table, f"{tempdir}/test.pq")
            table = pq.read_table(f"{tempdir}/test.pq")

            pl.from_arrow(table)

            os.system(f"ls -lh {tempdir}")
            dataset = ds.dataset(tempdir)

            # Load the PyArrow dataset into Polars, this will raise an error AttributeError: 'pyarrow.lib.StructArray' object has no attribute 'num_chunks'
            scanner = dataset.scanner(
                batch_size=DEFAULT_BATCH_READ_SIZE,
                use_threads=True,
                fragment_readahead=FRAGMENT_READ_AHEAD,
                batch_readahead=BATCH_READ_AHEAD,
            )

            for batch in scanner.to_batches():
                df = pl.from_arrow(batch)
                self.assertIsInstance(df, pl.DataFrame)
