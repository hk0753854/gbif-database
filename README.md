# GBIF Data Pipeline

GBIF（Global Biodiversity Information Facility）の生物観察データを取得し、
データ変換・品質チェック・Parquet保存・DuckDBによる集計までを行う
Python製のデータパイプラインです。

## Overview
このプロジェクトでは、GBIF Occurrence APIから生物の観察記録を取得し、

1. GBIF APIからデータを取得
2. Pandas DataFrameへ変換
3. データ品質をチェック
4. Parquet形式で保存
5. DuckDBでSQL分析
6. CLIから一連の処理を実行

というデータパイプラインを構築しています。

```text
GBIF API
   │
   ▼
gbif_client.py
   │
   ▼
transform.py
   │
   ▼
validate.py
   │
   ▼
Parquet
   │
   ▼
DuckDB
   │
   ├── Species別集計
   ├── Country別集計
   └── Year別集計
```

# Technologies
Python 3.12
Requests
Pandas
PyArrow
DuckDB
pytest
Ruff


# Project Structure
```
gbif-data-pipeline/
│
├── src/
│   └── gbif_data_pipeline/
│       ├── __init__.py
│       ├── gbif_client.py
│       ├── transform.py
│       ├── validate.py
│       ├── duckdb_client.py
│       ├── pipeline.py
│       └── cli.py
│
├── tests/
│   ├── test_cli.py
│   ├── test_duckdb_client.py
│   ├── test_gbif_client.py
│   ├── test_pipeline.py
│   ├── test_transform.py
│   └── test_validate.py
│
├── notebooks/
├── pyproject.toml
├── README.md
└── .gitignore
```

# Setup
1. Clone
```
git clone https://github.com/<your-account>/gbif-data-pipeline.git
cd gbif-data-pipeline
```
2. Create virtual environment
```
Windows:
    python -m venv .venv
```

3. Activate virtual environment
```
.venv\Scripts\Activate.ps1
```

4. Install
```
pip install -e .
Usage
```

以下のコマンドでGBIFから観察データを取得できます。
```
python -m gbif_data_pipeline.cli "Hynobius nebulosus" --limit 10
```
実行すると、取得したデータを
```
data/occurrences.parquet
```
として保存し、その後DuckDBを使用して集計します。

# Example Output
```
Pipeline completed: data\occurrences.parquet

=== Observations by Species ===
                                   scientificName  observations
0  Hynobius nebulosus (Temminck & Schlegel, 1838)            10


=== Observations by Country ===
  countryCode  observations
0          JP            10


=== Observations by Year ===
   year  observations
0  2022             1
1  2023             2
2  2026             7
```

# Data Processing
1. Data Collection
gbif_client.py ではGBIF Occurrence APIを利用して
生物観察データを取得します。

検索対象の学名を指定してデータを取得できます。

また、APIのpaginationにも対応しています。

2. Transformation
transform.py では、GBIF APIから取得したJSONデータを
Pandas DataFrameへ変換します。

主なカラム：
```
gbifID
scientificName
species
kingdom
phylum
class
order
family
genus
country
countryCode
decimalLatitude
decimalLongitude
eventDate
basisOfRecord
occurrenceStatus
```

3. Data Validation
validate.py では、分析に必要な最低限のカラムが存在するかを
チェックします。

現在の必須カラム：
```
gbifID
scientificName
countryCode
```

4. Parquet
取得したデータはParquet形式で保存します。
```
data/occurrences.parquet
```
Parquetを採用することで、DuckDBから直接データを読み込んで
SQLによる分析を行える構成にしています。


5. DuckDB
duckdb_client.py ではParquetに対してSQLを実行します。
現在、以下の集計を実装しています。
```
Species別観察件数
Country別観察件数
Year別観察件数
```
例えばSpecies別集計では、
```
SELECT
    scientificName,
    COUNT(*) AS observations
FROM read_parquet(?)
GROUP BY scientificName
ORDER BY observations DESC
```
というSQLを使用しています。

# Testing
pytestによるユニットテストを実装しています。

pytest結果　⇒　現在、11 passed

テスト対象：
```
GBIF API client
API pagination
DataFrame transformation
Data validation
Pipeline
DuckDB query
CLI
```

APIアクセス部分ではpytestのmonkeypatchを使用し、
実際のGBIF APIへアクセスせずにテストできる構成にしています。

# Design

このプロジェクトでは、各処理の責務を分離しています。
```
gbif_client.py
      │
      │ APIから取得
      ▼
transform.py
      │
      │ DataFrameへ変換
      ▼
validate.py
      │
      │ データ品質チェック
      ▼
pipeline.py
      │
      │ Parquet保存
      ▼
duckdb_client.py
      │
      │ SQL分析
      ▼
     結果
```
処理を分離することで、各コンポーネントを独立して
テスト・変更できる構成を目指しています。

# Future Improvements

今後は以下の機能追加を予定しています。
```
 APIリトライ処理
 ロギング
 複数speciesの一括取得
 増分取得
 データ品質チェックの強化
 Docker対応
 GitHub ActionsによるCI
 より高度なDuckDB分析
 パイプライン実行結果のログ管理
Learning Objectives
```
このプロジェクトを通して、以下のデータエンジニアリング技術を
実践しています。
```
REST APIからのデータ取得
ETLパイプライン構築
Pandasによるデータ変換
Parquetによるデータ保存
DuckDBによるSQL分析
データ品質チェック
pytestによるテスト
CLIアプリケーション
Pythonプロジェクト構成
Git / GitHubによるソースコード管理
```
# License
This project is for educational and portfolio purposes.