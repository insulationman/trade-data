# src/my_project/config.py
from pathlib import Path
import yaml
from dataclasses import dataclass
from typing import Any, Dict

CONFIG_PATH = Path(__file__).resolve().parents[0] / "config.yaml"


@dataclass(frozen=True)
class AppConfig:
    """Immutable container for the parts of the config you care about."""
    db_folder: str
    db_name: str
    baci_directory: str
    json_export_path: str

    @staticmethod
    def from_dict(raw: Dict[str, Any]) -> "AppConfig":
        db_cfg = raw.get("database", {})
        baci_cfg = raw.get("baci", {})
        json_cfg = raw.get("json", {})
        return AppConfig(
            db_folder=Path(db_cfg.get("dir", "./data/db")).expanduser().resolve().as_posix(),
            db_name=db_cfg.get("name", "my_app.sqlite"),
            baci_directory=Path(baci_cfg.get("dir", "./data/baci")).expanduser().resolve().as_posix(),
            json_export_path=Path(json_cfg.get("dir", "./data/json2")).expanduser().resolve().as_posix(),
        )


def load_config() -> AppConfig:
    """Read config.yaml and return an immutable AppConfig. If the file does not exist, throw an error."""
    if CONFIG_PATH.is_file():
        with CONFIG_PATH.open("r") as f:
            raw = yaml.safe_load(f) or {}
    else:
        #throw error and end execution
        raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH.as_posix()}")
        
    return AppConfig.from_dict(raw)