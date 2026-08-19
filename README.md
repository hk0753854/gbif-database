# GBIF Data Pipeline

GBIF（Global Biodiversity Information Facility）の生物多様性データを題材にした、**Databricksベースのデータエンジニアリングパイプライン**です。

GBIF Occurrence APIから観察データを取得し、Databricks上で **Bronze / Silver / Gold Architecture** に基づいてデータを処理・分析します。

また、ローカル開発環境では **Python / Pandas / Parquet / DuckDB** を利用し、Databricks環境では **PySpark / Delta Lake / Databricks Jobs** を利用する構成としています。

Databricks Jobは **Databricks Asset BundlesによるJob as Code** としてGit管理し、**GitHub ActionsによるCI/CD** まで実装しています。

---

## Overview

このプロジェクトでは、GBIF Occurrence APIから生物の観察記録を取得し、以下のデータパイプラインを構築しています。

```text
GBIF Occurrence API
        │
        ▼
    Data Ingestion
        │
        ▼
      Bronze
        │
        ▼
      Silver
        │
        ▼
       Gold
        │
        ▼
   Data Analysis
```

主な実装内容：

1. GBIF Occurrence APIから観察データを取得
2. Raw JSONをBronze Delta Tableへ保存
3. PySparkによるデータ変換
4. データ品質チェック
5. Silver Delta Tableへ構造化データを保存
6. Gold Layerで分析用テーブルを作成
7. Databricks Jobによる処理のオーケストレーション
8. Databricks Asset BundlesによるJob定義・デプロイ
9. GitHub ActionsによるCI/CD
10. Job Parametersによる取得対象・取得件数の制御
11. pytestによるユニットテスト
12. Ruffによるコード品質チェック

---

# Key Features

## Databricks Lakehouse Architecture

Databricks上でBronze / Silver / Goldの3層構成を実装しています。

```text
                    GBIF Occurrence API
                             │
                             ▼
                  ┌────────────────────┐
                  │ 01 Ingest          │
                  │ Python / Requests  │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Bronze             │
                  │ Raw JSON           │
                  │ Delta Table        │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ 02 Transform       │
                  │ PySpark            │
                  │ Data Quality       │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Silver             │
                  │ Structured Data    │
                  │ Delta Table        │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ 03 Gold            │
                  │ Analytical Tables  │
                  └────────────────────┘
```

## Job as Code

Databricks JobをGUIだけで構築するのではなく、Databricks Asset Bundlesを利用してGit管理しています。

```text
GitHub
  │
  ▼
databricks.yml
  │
  ▼
resources/gbif_pipeline.yml
  │
  ▼
Databricks Job
```

これにより、Jobの構成・Notebook・依存関係・パラメータをコードとして再現可能にしています。

## CI/CD

GitHub Actionsを利用して、Pull Request時の検証と`main`ブランチへのPush時のDatabricksデプロイを自動化しています。

```text
Pull Request
     │
     ├── Ruff
     ├── pytest
     └── Bundle Validate
     
main Push
     │
     ├── Ruff
     ├── pytest
     ├── Bundle Validate
     │
     └── Bundle Deploy
              │
              ▼
         Databricks
```

---

# Architecture

## Data Pipeline

GBIF Occurrence APIから取得したデータをDatabricks上で段階的に処理します。

```text
                         GBIF
                   Occurrence API
                         │
                         ▼
              ┌─────────────────────┐
              │ Ingestion            │
              │ Python / Requests    │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │ Bronze              │
              │ Raw JSON            │
              │ Delta Lake          │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │ Silver              │
              │ PySpark             │
              │ Structured Data     │
              │ Data Quality        │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │ Gold                │
              │ Analytical Tables   │
              └─────────────────────┘
```

---

# Databricks Pipeline

Databricks上では、3つのNotebookを依存関係付きのJobとして実行しています。

```text
01_ingest_gbif_to_bronze
            │
            ▼
02_transform_bronze_to_silver
            │
            ▼
03_create_gold_tables
```

## 01. Ingest

GBIF Occurrence APIから観察データを取得します。

Notebookでは以下のJob Parametersを指定できます。

```text
scientific_name
limit
```

例：

```text
scientific_name = Hynobius nebulosus
limit = 10
```

Job ParametersはDatabricks Asset Bundlesで定義し、Notebook Widgetへ渡しています。

```text
Job Parameters
      │
      ├── scientific_name
      │
      └── limit
             │
             ▼
01_ingest_gbif_to_bronze
```

取得したAPIレスポンスはRaw JSONとしてBronze Delta Tableへ保存します。

```text
workspace.bronze.gbif_occurrences
```

Bronze Layerでは可能な限りRawデータを保持することで、後続処理で必要な項目を追加できる構成としています。

---

## 02. Bronze → Silver

Bronzeに保存したJSONデータをPySparkで構造化します。

主な項目：

```text
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
query_scientific_name
ingested_at
```

`eventDate`については、分析しやすいように以下の項目へ分離しています。

```text
event_year
event_month
event_day
```

Silver Delta Table：

```text
workspace.silver.gbif_occurrences
```

---

# Data Quality

Silver Layerへの変換時に基本的なデータ品質チェックを実施しています。

現在チェックしている項目：

* `gbifID` の欠損
* `scientificName` の欠損
* `countryCode` の欠損
* 緯度の範囲チェック（-90 ～ 90）
* 経度の範囲チェック（-180 ～ 180）

```text
Bronze
   │
   ▼
PySpark Transformation
   │
   ▼
Data Quality Checks
   │
   ├── Required Columns
   ├── Latitude Range
   └── Longitude Range
   │
   ▼
Silver
```

単純なデータ変換だけではなく、分析前にデータ品質を確認する構成としています。

---

# Gold Layer

Silver Layerから分析用途の集計テーブルを作成します。

現在、以下のGold Tableを実装しています。

| Table                      | Description    |
| -------------------------- | -------------- |
| `species_summary`          | Species別の観察件数  |
| `country_summary`          | Country別の観察件数  |
| `year_summary`             | 年別の観察件数        |
| `observation_type_summary` | 観察タイプ別の件数      |
| `geographic_summary`       | 緯度・経度による観察地点集計 |

## Geographic Summary

緯度・経度を約0.1度単位に丸めてグリッド化し、近い観察地点をまとめています。

```text
decimalLatitude
decimalLongitude
        │
        ▼
  Grid Transformation
        │
        ▼
geographic_summary
```

これにより、観察地点の分布を簡易的に分析できるデータセットを作成しています。

---

# Databricks Job as Code

Databricks JobはGUIだけで構築するのではなく、Databricks Asset Bundlesを利用してコードとして定義しています。

Job定義：

```text
resources/gbif_pipeline.yml
```

Bundle設定：

```text
databricks.yml
```

Job構成：

```text
gbif_pipeline
│
├── ingest_gbif_to_bronze
│
├── transform_bronze_to_silver
│      └── depends_on:
│          ingest_gbif_to_bronze
│
└── create_gold_tables
       └── depends_on:
           transform_bronze_to_silver
```

Job Parameters：

```text
scientific_name
limit
```

これらのパラメータはNotebook Taskへ渡され、GBIF APIから取得する対象種・取得件数を制御します。

---

## Bundle Commands

Databricks CLIを使用して、Gitで管理されたJob定義をDatabricksへデプロイできます。

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run gbif_pipeline -t dev
```

これにより、Databricks Jobの構成をコードとして再現可能にしています。

---

# CI/CD

GitHub Actionsを利用して、PythonコードとDatabricks Bundleの自動検証・デプロイを実装しています。

## Pull Request

Pull Requestでは以下を実行します。

```text
Pull Request
     │
     ├── Ruff
     ├── pytest
     └── databricks bundle validate
```

コード品質・ユニットテスト・Databricks Job定義を検証し、問題がある場合はデプロイを行いません。

## mainへのPush

`main`ブランチへのPushでは、上記の検証に加えてDatabricksへのBundleデプロイを実行します。

```text
main push
   │
   ├── Ruff
   ├── pytest
   ├── bundle validate
   │
   └── bundle deploy -t dev
              │
              ▼
         Databricks
```

これにより、GitHubをソースコードのSingle Source of Truthとして、Databricks Jobの構成を自動的に反映できるようにしています。

---

# Technology Stack

| Category              | Technology               |
| --------------------- | ------------------------ |
| Language              | Python 3.12              |
| API                   | GBIF Occurrence API      |
| Data Processing       | PySpark                  |
| Data Platform         | Databricks               |
| Storage               | Delta Lake               |
| Local Data Processing | Pandas                   |
| Local Storage         | Parquet                  |
| Local SQL Engine      | DuckDB                   |
| Testing               | pytest                   |
| Linting               | Ruff                     |
| Version Control       | Git / GitHub             |
| CI/CD                 | GitHub Actions           |
| Job Deployment        | Databricks Asset Bundles |
| CLI                   | Databricks CLI           |

---

# Project Structure

```text
gbif-data-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── notebooks/
│   └── databricks/
│       ├── 01_ingest_gbif_to_bronze
│       ├── 02_transform_bronze_to_silver
│       └── 03_create_gold_tables
│
├── resources/
│   └── gbif_pipeline.yml
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
├── databricks.yml
├── config.yml
├── pyproject.toml
├── README.md
└── .gitignore
```

---

# Local Development

Databricks版とは別に、ローカル環境でもGBIF APIからデータを取得し、Parquet・DuckDBによるデータ処理・分析を実行できます。

ローカル開発環境とDatabricks実行環境を分離することで、開発・テストをローカルで行いながら、実行環境ではDatabricksの分散処理基盤を利用する構成としています。

## Setup

### 1. Clone

```bash
git clone https://github.com/hk0753854/animal-database.git
cd animal-database
```

### 2. Create Virtual Environment

Windows：

```bash
python -m venv .venv
```

### 3. Activate

```powershell
.venv\Scripts\Activate.ps1
```

### 4. Install Dependencies

```bash
pip install -e ".[dev]"
```

### 5. Run

```bash
python -m gbif_data_pipeline.cli "Hynobius nebulosus" --limit 10
```

ローカル実行では取得したデータをParquetとして保存し、DuckDBからSQLによる集計を行います。

---

# Local Data Processing

ローカル版では以下の処理を実装しています。

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

これにより、Databricksを利用しない環境でも基本的なデータ取得・変換・品質チェック・分析処理を確認できます。

---

# Testing

pytestによるユニットテストを実装しています。

現在のテスト対象：

```text
GBIF API Client
API Pagination
DataFrame Transformation
Data Validation
Pipeline
DuckDB Query
CLI
```

APIアクセス部分ではpytestの`monkeypatch`を使用し、実際のGBIF APIへアクセスせずにテストできる構成にしています。

```bash
pytest
```

RuffによるLintも実行しています。

```bash
ruff check src tests
```

これらのチェックはGitHub Actionsでも自動実行されます。

---

# Design

このプロジェクトでは、データ取得・変換・品質チェック・分析処理の責務を分離しています。

## Local Architecture

ローカル版では以下の構成としています。

```text
gbif_client.py
      │
      ▼
transform.py
      │
      ▼
validate.py
      │
      ▼
pipeline.py
      │
      ▼
duckdb_client.py
```

## Databricks Architecture

Databricks版では、ローカルで実装したデータ処理の考え方をLakehouse Architectureへ発展させています。

```text
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
```

さらにDatabricks JobをAsset Bundlesとしてコード管理することで、Notebook・Job・依存関係・パラメータをGitで管理できる構成としています。

---

# Development Philosophy

このプロジェクトでは、単にDatabricks上でNotebookを実行するだけではなく、実際のデータエンジニアリング開発を想定して以下を意識しています。

### 1. Local Development

Pythonによるデータ処理をローカル環境で開発・テストします。

### 2. Automated Testing

pytestによるユニットテストを実装し、APIやデータ処理ロジックを自動検証します。

### 3. Code Quality

RuffによるLintをCI/CDへ組み込み、コード品質を自動チェックします。

### 4. Lakehouse Architecture

Databricks上ではBronze / Silver / Goldにデータを分離します。

### 5. Job as Code

Databricks JobをAsset Bundlesで定義し、Gitによるバージョン管理を行います。

### 6. CI/CD

GitHub ActionsからBundleのValidation・Deploymentを自動実行します。

```text
Local Development
       │
       ▼
      Git
       │
       ▼
GitHub Actions
       │
       ├── Ruff
       ├── pytest
       └── Bundle Validate
              │
              ▼
       Bundle Deploy
              │
              ▼
         Databricks
              │
              ▼
       Production-like
        Data Pipeline
```

---

# Future Improvements

今後は以下の機能追加を予定しています。

```text
GBIF APIの大規模Pagination対応
増分取得
APIリトライ処理
重複データの検出
データ品質チェックの強化
Databricks SQL Dashboard
Azure DevOps Pipelineとの連携
パイプライン実行結果の監視・ログ管理
より高度な地理空間分析
dev / prod環境の分離
```

CI/CDによる自動テスト・Bundle Validation・Databricksへの自動デプロイについては実装済みです。

---

# Learning Objectives

このプロジェクトを通して、以下のデータエンジニアリング技術を実践しています。

```text
REST APIからのデータ取得
ETL / ELTパイプライン構築
Pythonによるデータ処理
PySparkによるデータ処理
Bronze / Silver / Gold Architecture
Delta Lake
Databricks Jobs
Databricks Asset Bundles
Job as Code
Job Parameters
データ品質チェック
Parquetによるデータ保存
DuckDBによるSQL分析
pytestによるテスト
RuffによるLint
Git / GitHubによるソースコード管理
GitHub ActionsによるCI/CD
```

---

# Portfolio Highlights

このプロジェクトでは、以下のデータエンジニアリング要素を一通り実装しています。

| Area                   | Implementation                     |
| ---------------------- | ---------------------------------- |
| Data Source            | GBIF Occurrence API                |
| Ingestion              | Python / Requests                  |
| Local Processing       | Pandas                             |
| Distributed Processing | PySpark                            |
| Data Lakehouse         | Databricks / Delta Lake            |
| Architecture           | Bronze / Silver / Gold             |
| Data Quality           | Required Fields / Range Validation |
| Analytics              | Gold Aggregation Tables            |
| Orchestration          | Databricks Jobs                    |
| Infrastructure as Code | Databricks Asset Bundles           |
| Parameterization       | Databricks Job Parameters          |
| Testing                | pytest                             |
| Code Quality           | Ruff                               |
| CI/CD                  | GitHub Actions                     |
| Version Control        | Git / GitHub                       |
| Local Analytics        | DuckDB                             |

---

# License

This project is for educational and portfolio purposes.
