import graphviz


def make_graph(clusters):
    G = graphviz.Digraph(
        graph_attr=dict(
            label="Data Model\n\n",
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

    COLORS = {
        "komet_sales_backend": "#E76B74",
        "source": "#E76B74",
        "staging": "#2bb59a",
        "intermediate": "#d7af70",
        "marts": "#5F0A87",
    }

    for cluster, nodes in clusters.items():
        with G.subgraph(
            name=f"cluster_{cluster}" if cluster not in ("intermediate", "marts") else cluster,
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
            C.attr(rank="same")
            for node in nodes:
                C.node(
                    node.unique_id,
                    node.name,
                    tooltip=node.compiled_sql or "",
                )

    for cluster, nodes in clusters.items():
        for node in nodes:
            if node.depends_on is not None:
                for parent in node.depends_on:
                    G.edge(parent.replace(".", "_"), node.unique_id)

    return G
