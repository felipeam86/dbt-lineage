from collections import defaultdict
from dataclasses import dataclass


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
    def from_model(cls, node):
        return cls(
            unique_id=node["unique_id"].replace(".", "_"),
            name=node["name"],
            description=node["description"],
            resource_type=node["resource_type"],
            fqn=node["fqn"],
            cluster=node["fqn"][1],
            raw_sql=node["raw_sql"],
            compiled_sql=node["compiled_sql"],
            depends_on=node["depends_on"]["nodes"],
        )

    @classmethod
    def from_source(cls, node):
        return cls(
            unique_id=node["unique_id"].replace(".", "_"),
            name=node["name"],
            description=node["description"],
            resource_type=node["resource_type"],
            fqn=node["fqn"],
            cluster=node["fqn"][1],
        )


def get_cluster(manifest):


    clusters = defaultdict(list)
    for _, node_json in manifest["nodes"].items():
        if node_json["resource_type"] == "model":
            node = Node.from_model(node_json)
            clusters[node.cluster].append(node)
    for _, node_json in manifest["sources"].items():
        node = Node.from_source(node_json)
        clusters[node.cluster].append(node)

    return clusters
