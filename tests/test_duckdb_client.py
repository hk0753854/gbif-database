from pathlib import Path

import pandas as pd

from gbif_data_pipeline.duckdb_client import (
    count_by_country,
    count_by_species,
    count_by_year,
    query_parquet,
)


def test_query_parquet(tmp_path: Path):
    parquet_path = tmp_path / "test.parquet"

    df = pd.DataFrame(
        {
            "scientificName": [
                "Hynobius nebulosus",
                "Hynobius nebulosus",
                "Cynops pyrrhogaster",
            ],
            "countryCode": [
                "JP",
                "JP",
                "JP",
            ],
        }
    )

    df.to_parquet(parquet_path, index=False)

    sql = """
    SELECT
        scientificName,
        COUNT(*) AS observations
    FROM read_parquet(?)
    GROUP BY scientificName
    ORDER BY observations DESC
    """

    result = query_parquet(
        parquet_path,
        sql,
    )

    assert len(result) == 2
    assert result.iloc[0]["scientificName"] == "Hynobius nebulosus"
    assert result.iloc[0]["observations"] == 2


def test_count_by_country(tmp_path: Path):
    parquet_path = tmp_path / "occurrences.parquet"

    df = pd.DataFrame(
        {
            "gbifID": ["1", "2", "3"],
            "scientificName": [
                "Hynobius nebulosus",
                "Hynobius nebulosus",
                "Hynobius nebulosus",
            ],
            "countryCode": ["JP", "JP", "US"],
        }
    )

    df.to_parquet(parquet_path, index=False)

    result = count_by_country(str(parquet_path))

    assert result.iloc[0]["countryCode"] == "JP"
    assert result.iloc[0]["observations"] == 2


def test_count_by_year(tmp_path: Path):
    parquet_path = tmp_path / "occurrences.parquet"

    df = pd.DataFrame(
        {
            "eventDate": [
                "2024-01-10",
                "2024-05-20",
                "2025-03-15",
            ],
        }
    )

    df.to_parquet(parquet_path, index=False)

    result = count_by_year(str(parquet_path))

    assert len(result) == 2
    assert result.iloc[0]["year"] == 2024
    assert result.iloc[0]["observations"] == 2
    assert result.iloc[1]["year"] == 2025
    assert result.iloc[1]["observations"] == 1


def test_count_by_species(tmp_path: Path):
    parquet_path = tmp_path / "occurrences.parquet"

    df = pd.DataFrame(
        {
            "scientificName": [
                "Hynobius nebulosus",
                "Hynobius nebulosus",
                "Cynops pyrrhogaster",
            ],
        }
    )

    df.to_parquet(parquet_path, index=False)

    result = count_by_species(str(parquet_path))

    assert len(result) == 2
    assert result.iloc[0]["scientificName"] == "Hynobius nebulosus"
    assert result.iloc[0]["observations"] == 2