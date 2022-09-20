import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

from graphviz import Digraph

COLORS = {
    "komet_sales_backend": "#E76B74",
    "source": "#E76B74",
    "staging": "#2bb59a",
    "intermediate": "#d7af70",
    "marts": "#5F0A87",
}


def read_manifest(manifest_filepath: Union[str, Path]):
    return json.loads(Path(manifest_filepath).read_text())


def get_base_graph(title: str = "Data Model\n\n") -> Digraph:
    G = Digraph(
        graph_attr=dict(
            label=title,
            labelloc="t",
            fontname="Courier New",
            fontsize="20",
            layout="dot",
            rankdir="LR",
            newrank="true",
        ),
        node_attr=dict(
            style="rounded, filled",
            shape="rect",
            fontname="Courier New",
        ),
        edge_attr=dict(
            arrowsize="1",
            penwidth="2",
        ),
    )

    return G


@dataclass
class Node:
    unique_id: str
    name: str
    description: str
    resource_type: str
    fqn: list
    cluster: str
    depends_on: list = None
    raw_sql: str = None
    compiled_sql: str = None

    @classmethod
    def from_manifest(cls, node):
        return cls(
            unique_id=node["unique_id"].replace(".", "_"),
            name=node["name"],
            description=node["description"],
            resource_type=node["resource_type"],
            fqn=node["fqn"],
            cluster=node["fqn"][1],
            raw_sql=node.get("raw_sql"),
            compiled_sql=node.get("compiled_sql"),
            depends_on=node.get("depends_on", dict()).get("nodes", []),
        )

    def __repr__(self) -> str:

        return f"Node(cluster={self.cluster!r}, name={self.name!r}, resource_type={self.resource_type!r})"


@dataclass
class Graph:
    nodes: defaultdict(list)
    edges: List[tuple]

    @classmethod
    def from_manifest(cls, manifest):

        clusters = defaultdict(list)
        edges = []
        for _, node in {**manifest["nodes"], **manifest["sources"]}.items():
            if node["resource_type"] in ("model", "source"):
                node = Node.from_manifest(node)
                clusters[node.cluster].append(node)
                for parent in node.depends_on:
                    edges.append((parent.replace(".", "_"), node.unique_id))

        return cls(nodes=clusters, edges=edges)

    @classmethod
    def from_manifest_file(cls, manifest_filepath: Union[str, Path]):
        manifest = read_manifest(manifest_filepath)
        return cls.from_manifest(manifest)

    def to_dot(self):

        G = get_base_graph()

        for cluster, nodes in self.nodes.items():
            with G.subgraph(
                name=f"cluster_{cluster}"
                if cluster not in ("intermediate", "marts")
                else cluster,
                graph_attr=dict(
                    label=cluster,
                    style="rounded",
                ),
                node_attr=dict(
                    color=COLORS[cluster],
                    fillcolor=COLORS[cluster],
                    fontcolor="white",
                ),
            ) as C:
                C.attr(
                    rank="same" if cluster not in ("intermediate", "marts") else None
                )
                for node in nodes:
                    C.node(
                        node.unique_id,
                        node.name,
                        tooltip=node.compiled_sql or "",
                    )

        for parent, child in self.edges:
            G.edge(parent, child)

        return G
