import pandas as pd

REQUIRED_COLUMNS = [
    "gbifID",
    "scientificName",
    "countryCode",
]


def validate_occurrences(df: pd.DataFrame) -> None:
    """GBIF occurrence DataFrameのデータ品質をチェックする。"""

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )