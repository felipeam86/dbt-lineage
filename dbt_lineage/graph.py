import json
import tempfile
import webbrowser
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import List, Mapping, Union

from graphviz import Digraph

from . import palettes


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
            cluster="seed" if node["resource_type"] == "seed" else node["fqn"][1],
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
    cluster_colors: Mapping[str, str]
    fontcolor: str = "black"

    @classmethod
    def from_manifest(
        cls,
        manifest,
        palette=palettes.Pastel,
        fontcolor=None,
    ):

        clusters = defaultdict(list)
        edges = []
        for _, node in {**manifest["nodes"], **manifest["sources"]}.items():
            if node["resource_type"] in ("model", "source", "seed"):
                node = Node.from_manifest(node)
                clusters[node.cluster].append(node)
                for parent in node.depends_on:
                    edges.append((parent.replace(".", "_"), node.unique_id))

        return cls(
            nodes=clusters,
            edges=edges,
            cluster_colors=dict(zip(clusters, palette)),
            fontcolor=fontcolor,
        )

    @classmethod
    def from_manifest_file(
        cls,
        manifest_filepath: Union[str, Path],
        palette: List[str] = palettes.Pastel,
        fontcolor: str = None,
    ):
        manifest = json.loads(Path(manifest_filepath).read_text())
        return cls.from_manifest(manifest, palette=palette, fontcolor=fontcolor)

    def to_dot(
        self,
        title: str = "Data Model\n\n",
        shapes: Mapping[str, str] = None,
    ) -> Digraph:
        shapes = shapes or dict()
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
                    color=self.cluster_colors[cluster],
                    fillcolor=self.cluster_colors[cluster],
                    fontcolor=self.fontcolor,
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
                        shape=shapes.get(node.resource_type, "box"),
                    )

        for parent, child in self.edges:
            G.edge(parent, child)

        return G

    def export_svg(self, filepath: Union[str, Path] = "graph"):
        G = self.to_dot()
        return G.render(filepath, format="svg")

    def preview(self):
        filepath = self.export_svg(Path(tempfile.mktemp("graph.svg")))
        webbrowser.open(f"file://{filepath}")
