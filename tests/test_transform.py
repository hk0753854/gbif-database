import pandas as pd

from gbif_data_pipeline.transform import occurrences_to_dataframe


def test_occurrences_to_dataframe():
    occurrences = [
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
            "decimalLatitude": 35.6895,
            "decimalLongitude": 139.6917,
            "eventDate": "2026-01-22",
            "basisOfRecord": "HUMAN_OBSERVATION",
            "occurrenceStatus": "present",
        }
    ]

    df = occurrences_to_dataframe(occurrences)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

    assert df.iloc[0]["gbifID"] == "123456"
    assert df.iloc[0]["scientificName"] == "Hynobius nebulosus"
    assert df.iloc[0]["countryCode"] == "JP"


def test_occurrences_to_dataframe_selects_required_columns():
    occurrences = [
        {
            "gbifID": "123456",
            "scientificName": "Hynobius nebulosus",
            "countryCode": "JP",
            "someUnnecessaryField": "不要なデータ",
        }
    ]

    df = occurrences_to_dataframe(occurrences)

    assert "gbifID" in df.columns
    assert "scientificName" in df.columns
    assert "countryCode" in df.columns

    assert "someUnnecessaryField" not in df.columns