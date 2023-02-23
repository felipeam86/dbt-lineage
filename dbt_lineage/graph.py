import json
import tempfile
import webbrowser
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Union

from graphviz import Digraph

from . import palettes
from .settings import config


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
            unique_id=node["unique_id"],
            name=node["name"],
            description=node["description"],
            resource_type=node["resource_type"],
            fqn=node["fqn"],
            cluster="seed" if node["resource_type"] == "seed" else node["fqn"][1],
            raw_sql=node.get("raw_sql"),
            compiled_sql=node.get("compiled_sql"),
            depends_on=node.get("depends_on", dict()).get("nodes", []),
        )

    def __repr__(self) -> str:

        return f"Node(cluster={self.cluster!r}, name={self.name!r}, resource_type={self.resource_type!r})"


@dataclass
class Graph:
    clusters: defaultdict(list)
    nodes: Dict[str, Node]
    edges: List[tuple]
    manifest: Dict

    @classmethod
    def from_manifest(cls, manifest):

        clusters = defaultdict(list)
        nodes = {}
        edges = []
        for _, node_json in {**manifest["nodes"], **manifest["sources"]}.items():
            if node_json["resource_type"] in ("model", "source", "seed"):
                node = Node.from_manifest(node_json)
                nodes[node.unique_id] = node
                clusters[node.cluster].append(node.unique_id)
                for parent in node.depends_on:
                    edges.append((parent, node.unique_id))

        return cls(clusters=clusters, nodes=nodes, edges=edges, manifest=manifest)

    @classmethod
    def from_manifest_file(cls, manifest_filepath: Union[str, Path]):
        manifest = json.loads(Path(manifest_filepath).read_text())
        return cls.from_manifest(manifest)

    def get_node_from_name(self, node_name):
        for _, node in self.nodes.items():
            if node.name == node_name:
                return node

    def get_node_parents(self, node_name):
        node = self.get_node_from_name(node_name=node_name)
        selected_nodes = trace_connections(
            connection_map=self.manifest["parent_map"],
            unique_id=node.unique_id,
        )

        return selected_nodes

    def get_node_childs(self, node_name):
        node = self.get_node_from_name(node_name=node_name)
        selected_nodes = trace_connections(
            connection_map=self.manifest["child_map"],
            unique_id=node.unique_id,
        )

        return self.nodes.keys() & selected_nodes

    def subgraph(self, selected_nodes):
        new_clusters = {}
        for cluster, node_ids in self.clusters.items():
            new_node_ids = list(set(node_ids) & selected_nodes)
            if len(new_node_ids) > 0:
                new_clusters[cluster] = new_node_ids
        new_nodes = {unique_id: self.nodes[unique_id] for unique_id in selected_nodes}
        new_edges = [
            edge for edge in self.edges if len(set(edge) & selected_nodes) == 2
        ]
        return Graph(
            clusters=new_clusters,
            nodes=new_nodes,
            edges=new_edges,
            manifest=self.manifest,
        )

    def select(self, select: str):
        if select is not None:
            selected_nodes = set()
            node_name = select.replace("+", "")
            if (select[0] == "+") or ("+" not in select):
                selected_nodes = selected_nodes.union(self.get_node_parents(node_name))
            if select[-1] == "+":
                selected_nodes = selected_nodes.union(self.get_node_childs(node_name))

        return self.subgraph(selected_nodes)

    def to_dot(
        self,
        title: str = config.title,
        shapes: Mapping[str, str] = None,
        palette: List[str] = getattr(palettes, config.palette.name),
        fontcolor: str = config.fontcolor,
        subgraph_clusters: List = None,
    ) -> Digraph:

        shapes = shapes or config.shapes
        cluster_colors = dict(zip(self.clusters.keys(), palette))
        subgraph_clusters = subgraph_clusters or config.subgraph_clusters

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

        for cluster, node_ids in self.clusters.items():
            with G.subgraph(
                name=f"cluster_{cluster}" if cluster in subgraph_clusters else cluster,
                graph_attr=dict(
                    label=cluster,
                    style="rounded",
                ),
                node_attr=dict(
                    color=cluster_colors[cluster],
                    fillcolor=cluster_colors[cluster],
                    fontcolor=fontcolor,
                ),
            ) as C:
                C.attr(rank="same" if cluster in subgraph_clusters else None)
                for node_id in node_ids:
                    node = self.nodes[node_id]
                    C.node(
                        node.unique_id.replace(".", "_"),
                        node.name,
                        tooltip=getattr(node, config.tooltip) or "",
                        shape=shapes.get(node.resource_type, "box"),
                    )

        for parent, child in self.edges:
            G.edge(parent.replace(".", "_"), child.replace(".", "_"))

        return G

    def export_svg(self, filepath: Union[str, Path] = "graph"):
        G = self.to_dot()
        return G.render(filepath, format="svg")

    def preview(self):
        filepath = self.export_svg(Path(tempfile.mktemp("graph.svg")))
        webbrowser.open(f"file://{filepath}")


def trace_connections(connection_map, unique_id, selected=None):
    selected = selected or {unique_id}
    connections = connection_map[unique_id]
    if len(connections) == 0:
        return selected
    else:
        selected = selected.union(connections)
        for parent_unique_id in connections:
            selected = trace_connections(
                connection_map=connection_map,
                unique_id=parent_unique_id,
                selected=selected,
            )
    return selected
