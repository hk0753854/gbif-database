from pathlib import Path

from gbif_data_pipeline.config import load_config


def test_load_config(tmp_path: Path):
    config_path = tmp_path / "config.yaml"

    config_path.write_text(
        """
scientific_name: Hynobius nebulosus
limit: 100
output_path: data/occurrences.parquet
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config["scientific_name"] == "Hynobius nebulosus"
    assert config["limit"] == 100
    assert config["output_path"] == "data/occurrences.parquet"