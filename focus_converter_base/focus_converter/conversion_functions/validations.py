import io

import networkx as nx
import polars as pl
import sqlglot

from focus_converter.configs.base_config import ConversionPlan

SOURCE_COLUMN_NAME = "SOURCE"
SINK_COLUMN_NAME = "FOCUS_DATASET"
STATIC_VALUE_COLUMN = "STATIC_VALUE"


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

        source_columns = set(
            [
                column.alias_or_name
                for column in sqlglot.parse_one(sql_query).find_all(sqlglot.exp.Column)
            ]
        )
        self.__validate_column_names__(plan=plan, column_names=source_columns)

        target_columns = set(
            [
                column.alias_or_name
                for column in sqlglot.parse_one(sql_query).find_all(sqlglot.exp.Alias)
            ]
        )

        for source_column in source_columns:
            for target_column in target_columns:
                self.__network_graph__.add_edge(source_column, target_column, plan=plan)

        self.__add_sink_node__(focus_column=plan.focus_column.value)

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

    def validate_lazy_frame_columns(self, lf: pl.LazyFrame):
        # get all columns that have edge from source
        source_columns = [
            column for column in self.__network_graph__.successors(SOURCE_COLUMN_NAME)
        ]

        for source_columns in source_columns:
            if source_columns not in lf.columns:
                raise ValueError(f"Column {source_columns} not found in data")

    def generate_mermaid_uml(self):
        """
        Using networkx, generate a mermaid compatible UML graph of the conversion plan
        """

        graph_uml = io.StringIO()
        graph_uml.write("graph TD;\n")
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
