import tempfile
from unittest import TestCase

import polars
import polars as pl

from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats, ParquetDataFormat


class TestConnectedNodes(TestCase):
    def __genetic_fnc_connected_nodes__(self, provider):
        # the only terminal node should be FOCUS column, ensuring that all columns are transformed

        converter = FocusConverter()
        converter.load_provider_conversion_configs()
        converter.prepare_horizontal_conversion_plan(provider=provider)

        graph = converter.__network__.__prepare_graph__()
        leaf_nodes = [
            node for node, out_degree in graph.out_degree() if out_degree == 0
        ]

        for node in graph.nodes():
            print(node)
            print(list(graph.successors(node)))
            print()

        self.assertEqual(len(leaf_nodes), 1, "More than one terminal node found")
        self.assertEqual(
            set(leaf_nodes), {"Focus_Dataset"}, "Terminal node is not FOCUS column"
        )

    def test_validate_aws_plan(self):
        self.__genetic_fnc_connected_nodes__(provider="aws")

    #
    # def test_validate_gcp_plan(self):
    #     self.__genetic_fnc_connected_nodes__(provider="gcp")
    #
    # def test_validate_azure_plan(self):
    #     self.__genetic_fnc_connected_nodes__(provider="azure")
    #
    # def test_validate_oci_plan(self):
    #     self.__genetic_fnc_connected_nodes__(provider="oci")

    def test_columns_check(self):
        lazy_frame = pl.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]}).lazy()

        with tempfile.NamedTemporaryFile() as file:
            lazy_frame.collect().write_parquet(file.name)

            converter = FocusConverter(column_prefix=None)
            converter.load_provider_conversion_configs()
            converter.prepare_horizontal_conversion_plan(provider="gcp")
            converter.load_data(
                data_path=file.name,
                data_format=DataFormats.PARQUET,
                parquet_data_format=ParquetDataFormat.DATASET,
            )
            converter.configure_data_export(
                export_path="/tmp/converted_aws",
                export_include_source_columns=False,
            )
            pre = converter.__process_lazy_frame__(lazy_frame)
            print(pre)
            raise ValueError
            # print(pre.explain())

            # converter.convert()
            return
            pre = converter.__process_lazy_frame__(lazy_frame)
            try:
                pre.collect()
            except polars.exceptions.ColumnNotFoundError as e:
                print(type(e.args[:100]), len(e.args))
                print(len(e.args))
                print(len(e.args[0]))

                # print last 100 characters of the error message

                last_lines = e.args[0].splitlines()

                for line in last_lines[len(last_lines) - 100 : len(last_lines)]:
                    print(line)
                    # if "ErrorStateSync" in line:
                    #     print(line)
                pass
            raise ValueError(
                "Column not found in the dataframe. Please check the column names."
            )
