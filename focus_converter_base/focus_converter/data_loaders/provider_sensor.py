import logging

import pandas as pd
import polars as pl
import pyarrow
import pyarrow.dataset as ds

from focus_converter.common.cli_options import (
    DATA_FORMAT_OPTION,
    PARQUET_DATA_FORMAT_OPTION,
)
from focus_converter.conversion_functions import STATIC_CONVERSION_TYPES
from focus_converter.converter import FocusConverter


class ProviderSensor:
    """
    Senses provider automatically using first few rows of an input file.
    Can also distinguish between various file formats.
    """

    provider: str = None
    data_format: DATA_FORMAT_OPTION = None
    parquet_data_format: PARQUET_DATA_FORMAT_OPTION = None

    def __init__(self, base_path):
        self.__base_path__ = base_path

    def __try_load_csv_file__(self):
        df = pd.read_csv(self.__base_path__)
        return df.head(10)

    def __try_load_parquet_fragments__(self):
        dataset = ds.dataset(self.__base_path__)
        return pl.scan_pyarrow_dataset(dataset).head(10).collect().to_pandas()

    def __try_load_parquet__(self):
        return pl.scan_parquet(self.__base_path__).head(10).collect().to_pandas()

    def __sense_file_format__(self):
        try:
            data_sample = self.__try_load_parquet_fragments__()
            self.data_format = DATA_FORMAT_OPTION.PARQUET
            self.parquet_data_format = PARQUET_DATA_FORMAT_OPTION.DATASET
            return data_sample
        except pyarrow.lib.ArrowInvalid as e:
            logging.debug(f"Not parquet dataset, {str(e)}")

        try:
            data_sample = self.__try_load_parquet__()
            self.data_format = DATA_FORMAT_OPTION.PARQUET
            self.parquet_data_format = PARQUET_DATA_FORMAT_OPTION.FILE
            return data_sample
        except pl.exceptions.ComputeError as e:
            logging.debug(f"Not parquet file, {str(e)}")

        try:
            data_sample = self.__try_load_csv_file__()
            self.data_format = DATA_FORMAT_OPTION.CSV
            return data_sample
        except Exception as e:
            logging.debug(f"Not csv file, {str(e)}")

        raise RuntimeError(
            f"Exhausted all read methods, no suitable file format found for input: {self.__base_path__}"
        )

    def __sense_provider__(self, data_sample):
        computed_provider_name = None
        matched_column_names_count = 0

        converter = FocusConverter()
        converter.load_provider_conversion_configs()
        for provider_name in converter.plans.keys():
            for plan in converter.plans[provider_name]:
                if plan.conversion_type == STATIC_CONVERSION_TYPES.SET_COLUMN_DTYPES:
                    plan_column_names = [
                        column_obj["column_name"]
                        for column_obj in plan.conversion_args["dtype_args"]
                    ]
                    matched_columns = set(plan_column_names) & set(data_sample.columns)
                    if len(matched_columns) > matched_column_names_count:
                        computed_provider_name = provider_name
                        matched_column_names_count = len(matched_columns)

        if computed_provider_name is None:
            raise RuntimeError(
                "Failed to find a suitable provider for the given dataset, either provider is not configured yet or data might be corrupted."
            )

        self.provider = computed_provider_name

    def load(self):
        data_sample = self.__sense_file_format__()
        self.__sense_provider__(data_sample=data_sample)
