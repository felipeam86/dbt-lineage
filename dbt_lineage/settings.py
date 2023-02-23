from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List

from omegaconf import OmegaConf

DEFAULT_CONFIG_FILE = Path(__file__).parent / "default_config.yml"
USER_CONFIG_FILE = Path("dbt_lineage.yml")


DEFAULT_SHAPES = {
    "source": "cds",
    "seed": "component",
    "model": "box",
}


class Palette(Enum):
    Vivid = "Vivid"
    Bold = "Bold"
    Prism = "Prism"
    Pastel = "Pastel"


@dataclass
class Config:
    title: str
    tooltip: str
    palette: Palette
    fontcolor: str
    subgraph_clusters: List
    shapes: Dict[str, str]


config = OmegaConf.load(DEFAULT_CONFIG_FILE)

if USER_CONFIG_FILE.exists():
    config = OmegaConf.merge(
        config,
        OmegaConf.load(USER_CONFIG_FILE),
    )

schema = OmegaConf.structured(Config)
config = OmegaConf.merge(schema, config)
