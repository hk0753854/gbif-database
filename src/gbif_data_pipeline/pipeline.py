from pathlib import Path

from gbif_data_pipeline.gbif_client import search_occurrences
from gbif_data_pipeline.transform import occurrences_to_dataframe
from gbif_data_pipeline.validate import validate_occurrences


def run_pipeline(
    scientific_name: str,
    limit: int = 100,
    output_path: str | Path = "data/occurrences.parquet",
) -> Path:
    """GBIFからデータを取得し、検証してParquetとして保存する。"""

    # 1. GBIFから取得
    occurrences = search_occurrences(
        scientific_name=scientific_name,
        limit=limit,
    )

    # 2. DataFrameへ変換
    df = occurrences_to_dataframe(occurrences)

    # 3. データ品質チェック
    validate_occurrences(df)

    # 4. 出力先ディレクトリを作成
    output_path = Path(output_path)
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 5. Parquetとして保存
    df.to_parquet(
        output_path,
        index=False,
    )

    return output_path