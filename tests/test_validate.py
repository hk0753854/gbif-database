import pandas as pd
import pytest

from gbif_data_pipeline.validate import validate_occurrences


def test_validate_occurrences_success():
    df = pd.DataFrame(
        {
            "gbifID": ["123"],
            "scientificName": ["Hynobius nebulosus"],
            "countryCode": ["JP"],
        }
    )

    validate_occurrences(df)


def test_validate_occurrences_missing_column():
    df = pd.DataFrame(
        {
            "gbifID": ["123"],
            "scientificName": ["Hynobius nebulosus"],
        }
    )

    with pytest.raises(ValueError, match="Required columns are missing"):
        validate_occurrences(df)