import base64
import io

import networkx as nx
import polars as pl
import requests
import sqlglot

from focus_converter.configs.base_config import (
    ConversionPlan,
    SetColumnDTypesConversionArgs,
)

SOURCE_COLUMN_NAME = "SOURCE"
SINK_COLUMN_NAME = "FOCUS_DATASET"
STATIC_VALUE_COLUMN = "STATIC_VALUE"


def mm(graph):
    # generates UML using mermaid's public api
    # TODO: Find a robust local graph draw

    graphbytes = graph.encode("ascii")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    return requests.get(f"https://mermaid.ink/img/{base64_string}").content


class ColumnValidator:
    def __init__(self):
        self.__network_graph__ = network_graph = nx.DiGraph()
        network_graph.add_node(SOURCE_COLUMN_NAME)
        network_graph.add_node(SINK_COLUMN_NAME)

    def __validate_column_names__(self, plan: ConversionPlan, column_names):
        # ensures that dependent columns are present in the data

        for column_name in column_names:
            if (
                column_name not in self.__network_graph__.nodes
                and column_name != STATIC_VALUE_COLUMN
            ):
                # add edge indicating that these are to be provided from source data
                # if the edge not present, then it is expected to come from the source dataset
                self.__network_graph__.add_edge(SOURCE_COLUMN_NAME, column_name)

    def __add_sink_node__(self, focus_column):
        self.__network_graph__.add_edge(focus_column, SINK_COLUMN_NAME)

    def map_sql_query(self, sql_query, plan: ConversionPlan):
        """
        Maps a sql query to the graph

        :param sql_query: str, sql query to be mapped
        :param plan: ConversionPlan, plan to be added to the graph
        """

        # generate a list of all columns that are aliased
        source_columns = sorted(
            set(
                [
                    column.alias_or_name
                    for column in sqlglot.parse_one(sql_query).find_all(
                        sqlglot.exp.Column
                    )
                ]
            )
        )
        self.__validate_column_names__(plan=plan, column_names=source_columns)

        # generate sorted list of target columns from the query
        target_columns = sorted(
            set(
                [
                    column.alias_or_name
                    for column in sqlglot.parse_one(sql_query).find_all(
                        sqlglot.exp.Alias
                    )
                ]
            )
        )

        for source_column in source_columns:
            for target_column in target_columns:
                self.__network_graph__.add_edge(source_column, target_column, plan=plan)

        # analyze target_columns that are focus columns and automatically add sink node
        for target_column in target_columns:
            if target_column == plan.focus_column.value:
                self.__add_sink_node__(focus_column=target_column)

    def map_non_sql_plan(
        self, plan: ConversionPlan, column_alias, source_column: str = None
    ):
        """
        Maps a non sql plan to the graph

        :param plan: ConversionPlan, plan to be added to the graph
        :param column_alias: str, alias to be used for the column
        :param source_column: str: Optional source column name if different from plan.column, used for unnest
        :return:
        """

        self.__validate_column_names__(
            plan=plan, column_names=[source_column or plan.column]
        )
        self.__network_graph__.add_edge(
            source_column or plan.column, column_alias, plan=plan
        )
        self.__add_sink_node__(focus_column=plan.focus_column.value)

    def map_static_default_value_if_not_present(
        self, plan: ConversionPlan, column_alias
    ):
        self.__network_graph__.add_edge(SOURCE_COLUMN_NAME, plan.column, plan=plan)

    def map_dtype_enforced_node(self, plan: ConversionPlan):
        conversion_args = SetColumnDTypesConversionArgs.model_validate(
            plan.conversion_args
        )
        for column_obj in conversion_args.dtype_args:
            self.__network_graph__.add_edge(
                SOURCE_COLUMN_NAME, column_obj.column_name, plan=plan
            )

    def validate_lazy_frame_columns(self, lf: pl.LazyFrame):
        # get all columns that have edge from source
        source_columns = [
            column for column in self.__network_graph__.successors(SOURCE_COLUMN_NAME)
        ]

        columns_missing = sorted(set(source_columns) - set(lf.columns))
        if columns_missing:
            raise ValueError(
                f"Column(s) '{', '.join(columns_missing)}' not found in data"
            )

    def validate_graph_is_connected(self):
        """
        Validates that the graph is connected
        """

        graph = self.__network_graph__

        sink_nodes = [node for node in graph.nodes() if graph.out_degree(node) == 0]
        sink_nodes = set(sink_nodes) - {SOURCE_COLUMN_NAME, SINK_COLUMN_NAME}

        if len(sink_nodes) > 0:
            raise ValueError(
                "Following sink nodes are not connected, potentially missing transform steps",
                sink_nodes,
            )

    def generate_mermaid_uml(self):
        """
        Using networkx, generate a mermaid compatible UML graph of the conversion plan
        """

        graph_uml = io.StringIO()
        graph_uml.write("graph LR;\n")

        for source, target, edge_data in self.__network_graph__.edges(data=True):
            plan: ConversionPlan = edge_data.get("plan")
            if plan:
                graph_uml.write(
                    f"\t{source} -- {plan.conversion_type.name}:{plan.config_file_name} --> {target}\n"
                )
            else:
                graph_uml.write(f"\t{source} --> {target}\n")

        graph_uml.seek(0)
        return graph_uml.read()

    def generate_uml_graph(self):
        mermaid_graph = self.generate_mermaid_uml()
        return mm(mermaid_graph)
