import shutil

import typer

from .graph import Graph
from .settings import DEFAULT_CONFIG_FILE, USER_CONFIG_FILE

app = typer.Typer()


def get_graph(manifest: str = "target/manifest.json", select: str = None):
    G = Graph.from_manifest_file(manifest)
    if select is not None:
        G = G.select(select)
    return G


@app.command()
def preview(
    manifest: str = "target/manifest.json",
    select: str = None,
):
    """Preview SVG file of graph on browser"""
    G = get_graph(manifest=manifest, select=select)
    G.preview()


@app.command()
def export(
    manifest: str = "target/manifest.json",
    filepath: str = "graph",
    select: str = None,
):
    """Export SVG file of graph"""
    G = get_graph(manifest=manifest, select=select)
    G.export_svg(filepath)


@app.command()
def init():
    """Create starter 'dbt_lineage.yml' config file with defaults"""
    if USER_CONFIG_FILE.exists():
        print("User config file alredy present. Will not overwrite")
        return
    shutil.copy(str(DEFAULT_CONFIG_FILE), str(USER_CONFIG_FILE))
