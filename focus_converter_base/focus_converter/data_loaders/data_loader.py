from enum import Enum
from typing import Iterable

import polars as pl
import pyarrow.dataset as ds
from tqdm import tqdm

# these values need to be tweaked, for datasets with small number of columns,
# following values can be much larger.
# for very fragments with large number of row groups, this will cause memory to exhaust
DEFAULT_BATCH_READ_SIZE = 50000
FRAGMENT_READ_AHEAD = 0
BATCH_READ_AHEAD = 0


class DataFormats(Enum):
    CSV = "csv"
    PARQUET = "parquet"


class ParquetDataFormat(Enum):
    FILE = "file"
    DATASET = "dataset"
    DELTA = "delta"


class DataLoader:
    def __init__(
        self,
        data_path: str,
        data_format: DataFormats,
        parquet_data_format: ParquetDataFormat = None,
    ):
        self.__data_path__ = data_path
        self.__data_format__ = data_format
        self.__parquet_data_format__ = parquet_data_format

    def load_pyarrow_dataset(self) -> Iterable[pl.LazyFrame]:
        dataset = ds.dataset(self.__data_path__)
        scanner = dataset.scanner(
            batch_size=DEFAULT_BATCH_READ_SIZE,
            use_threads=True,
            fragment_readahead=FRAGMENT_READ_AHEAD,
            batch_readahead=BATCH_READ_AHEAD,
        )

        total_rows = dataset.count_rows()

        with tqdm(total=total_rows) as pobj:
            for batch in scanner.to_batches():
                df = pl.from_arrow(batch)
                yield df.lazy()
                pobj.update(df.shape[0])

    def load_parquet_file(self) -> Iterable[pl.LazyFrame]:
        # reads parquet from data path and returns a lazy object

        yield pl.read_parquet(self.__data_path__).lazy()

    def load_csv(self) -> Iterable[pl.LazyFrame]:
        # reads csv from data path and returns a lazy object

        yield pl.read_csv(
            self.__data_path__, try_parse_dates=False, ignore_errors=True
        ).lazy()

    def data_scanner(self) -> Iterable[pl.LazyFrame]:
        # helper function to read from different data formats and create an iterator of lazy frames
        # which then can be used to apply lazy eval plans

        if self.__data_format__ == DataFormats.CSV:
            yield from self.load_csv()
        elif self.__data_format__ == DataFormats.PARQUET:
            if self.__parquet_data_format__ == ParquetDataFormat.FILE:
                yield from self.load_parquet_file()
            elif self.__parquet_data_format__ == ParquetDataFormat.DATASET:
                yield from self.load_pyarrow_dataset()
            else:
                raise NotImplementedError(
                    f"Parquet format:{self.__parquet_data_format__} not implemented"
                )
        else:
            raise NotImplementedError(
                f"Data format:{self.__data_format__} not implemented"
            )
