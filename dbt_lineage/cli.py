import shutil

import typer

from .graph import Graph
from .settings import DEFAULT_CONFIG_FILE, USER_CONFIG_FILE

app = typer.Typer()


@app.command()
def preview(manifest: str = "target/manifest.json"):
    """Preview SVG file of graph on browser"""
    G = Graph.from_manifest_file(manifest)
    G.preview()


@app.command()
def export(
    manifest: str = "target/manifest.json",
    filepath: str = "graph",
):
    """Export SVG file of graph"""
    G = Graph.from_manifest_file(manifest)
    G.export_svg(filepath)


@app.command()
def init():
    """Create starter 'dbt_lineage.yml' config file with defaults"""
    if USER_CONFIG_FILE.exists():
        print("User config file alredy present. Will not overwrite")
        return
    shutil.copy(str(DEFAULT_CONFIG_FILE), str(USER_CONFIG_FILE))
