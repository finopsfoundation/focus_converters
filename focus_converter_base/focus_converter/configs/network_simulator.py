import base64
import io
from itertools import groupby

import networkx as nx
import requests

from focus_converter.configs.base_config import ConversionPlan


def mm(graph):
    # generates UML using mermaid's public api
    # TODO: Find a robust local graph draw

    graphbytes = graph.encode("ascii")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    return requests.get(f"https://mermaid.ink/img/{base64_string}").content


class NetworkSimulator:
    def __init__(self):
        self.__plans__ = []

    def add_conversion_node(self, plan: ConversionPlan):
        # adds a plan edge that can be added to the graph when generating graphs

        sink_column_name = plan.focus_column.value
        if plan.column_prefix:
            sink_column_name = f"{plan.column_prefix}-{sink_column_name}"
        sink_column_name = f"{sink_column_name}.{plan.priority}"
        self.__plans__.append(
            {
                "source": plan.column,
                "sink": sink_column_name,
                "focus_column_name": plan.focus_column.value,
                "conversion_type": plan.conversion_type,
                "file_name": plan.config_file_name
            }
        )

    def show_graph(self):
        # creates a graph based on plans including intermediate steps with tmp columns

        graph = nx.DiGraph()

        for focus_column, plans in groupby(
            self.__plans__, key=lambda item: item["focus_column_name"]
        ):
            plans = list(plans)
            for i, plan in enumerate(plans):
                if i == 0:
                    graph.add_edge("Source_Dataset", plan["source"])
                    graph.add_edge(
                        plan["source"],
                        plan["sink"],
                        conversion_type=plan["conversion_type"].value,
                        file_name=plan['file_name']
                    )
                else:
                    graph.add_edge(
                        plans[i - 1]["sink"],
                        plan["sink"],
                        conversion_type=plan["conversion_type"].value,
                        file_name=plan['file_name']
                    )

        # collect all target nodes and add an edge connecting to exported dataset
        targets = [x for x in graph.nodes() if graph.out_degree(x) == 0]
        for target in targets:
            graph.add_edge(v_of_edge="Focus_Dataset", u_of_edge=target)

        graph_uml = io.StringIO()
        graph_uml.write("graph TD;\n")
        for source, target, edge_data in graph.edges(data=True):
            if edge_data:
                conversion_type = edge_data.get('conversion_type')
                if edge_data.get("file_name"):
                    conversion_type = f"{conversion_type}:{edge_data.get('file_name')}"
                graph_uml.write(
                    f"\t{source}-- {conversion_type} -->{target}\n"
                )
            else:
                graph_uml.write(f"\t{source}-->{target}\n")

        graph_uml.seek(0)
        return mm(graph_uml.read())
