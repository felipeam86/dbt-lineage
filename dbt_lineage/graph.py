from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

COLORS = {
    "source": "#E76B74",
    "staging": "#2bb59a",
    "intermediate": "#d7af70",
    "marts": "#5F0A87",
}


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


def get_cluster(manifest: str) -> Dict[str, List[Node]]:

    clusters = defaultdict(list)
    for _, node_json in {**manifest["nodes"], **manifest["sources"]}.items():
        if node_json["resource_type"] in ("model", "source"):
            node = Node.from_manifest(node_json)
            clusters[node.cluster].append(node)

    return clusters
