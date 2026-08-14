
import pandas as pd

from gbif_data_pipeline.pipeline import run_pipeline


def test_run_pipeline(monkeypatch, tmp_path):
    """GBIF取得からParquet保存までのパイプラインをテストする。"""

    fake_occurrences = [
        {
            "gbifID": "123456",
            "scientificName": "Hynobius nebulosus",
            "species": "Hynobius nebulosus",
            "kingdom": "Animalia",
            "phylum": "Chordata",
            "class": "Amphibia",
            "order": "Caudata",
            "family": "Hynobiidae",
            "genus": "Hynobius",
            "country": "Japan",
            "countryCode": "JP",
            "decimalLatitude": 35.6,
            "decimalLongitude": 139.7,
            "eventDate": "2026-01-22",
            "basisOfRecord": "HUMAN_OBSERVATION",
            "occurrenceStatus": "PRESENT",
        }
    ]

    def fake_search_occurrences(scientific_name, limit):
        assert scientific_name == "Hynobius nebulosus"
        assert limit == 10

        return fake_occurrences

    monkeypatch.setattr(
        "gbif_data_pipeline.pipeline.search_occurrences",
        fake_search_occurrences,
    )

    output_path = tmp_path / "occurrences.parquet"

    result = run_pipeline(
        scientific_name="Hynobius nebulosus",
        limit=10,
        output_path=output_path,
    )

    assert result == output_path
    assert output_path.exists()

    df = pd.read_parquet(output_path)

    assert len(df) == 1
    assert df.iloc[0]["gbifID"] == "123456"
    assert df.iloc[0]["species"] == "Hynobius nebulosus"
    assert df.iloc[0]["countryCode"] == "JP"