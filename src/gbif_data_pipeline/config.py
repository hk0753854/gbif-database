from pathlib import Path

import yaml


def load_config(config_path: str | Path) -> dict:
    """YAML形式の設定ファイルを読み込む。"""

    config_path = Path(config_path)

    with config_path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if config is None:
        return {}

    return config