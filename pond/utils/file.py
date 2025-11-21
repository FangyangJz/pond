from pathlib import Path
import json
import tomllib
from typing import Any


def load_config_dict(file_path: Path, key: str = "") -> dict[str, Any]:
    with open(file_path, "rb") as f:
        cfg_dict = tomllib.load(f)

    if key:
        return cfg_dict[key]
    else:
        return cfg_dict


def save_json(json_dict: dict[str, Any], save_path: Path):
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, "w") as f:
        json.dump(json_dict, f, indent=4)


def load_json(json_path: Path) -> dict[str, Any]:
    with open(json_path, "r") as f:
        return json.load(f)
