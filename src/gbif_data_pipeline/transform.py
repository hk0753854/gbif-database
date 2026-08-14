import pandas as pd

COLUMNS = [
    "gbifID",
    "scientificName",
    "species",
    "kingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "country",
    "countryCode",
    "decimalLatitude",
    "decimalLongitude",
    "eventDate",
    "basisOfRecord",
    "occurrenceStatus",
]


def occurrences_to_dataframe(
    occurrences: list[dict],
) -> pd.DataFrame:
    """GBIF occurrenceデータをDataFrameに変換する。"""

    df = pd.DataFrame(occurrences)

    # 存在しないカラムがあってもエラーにしない
    df = df.reindex(columns=COLUMNS)

    return df