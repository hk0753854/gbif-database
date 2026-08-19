# GBIF Data Pipeline

GBIF（Global Biodiversity Information Facility）の生物多様性データを題材にした、**Databricksベースのデータエンジニアリングパイプライン**です。

GBIF Occurrence APIから観察データを取得し、Databricks上で **Bronze / Silver / Gold Architecture** に基づいてデータを処理・分析します。

ローカル開発環境では **Python / Pandas / Parquet / DuckDB** を利用し、Databricks環境では **PySpark / Delta Lake / Databricks Jobs** を利用します。

また、Databricks Jobは **Databricks Asset BundlesによるJob as Code** としてGit管理し、**GitHub ActionsによるCI/CD**まで実装しています。

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

* GBIF Occurrence APIから観察データを取得
* APIエラーに対するRetry処理
* Raw JSONをBronze Delta Tableへ保存
* PySparkによるデータ変換
* データ品質チェック
* Silver Delta Tableへの構造化データ保存
* Gold Layerで分析用テーブルを作成
* Databricks Jobによるパイプラインのオーケストレーション
* Databricks Asset BundlesによるJob as Code
* Job Parametersによる取得対象・取得件数の制御
* GitHub ActionsによるCI/CD
* pytestによるユニットテスト
* Ruffによるコード品質チェック
* ローカル環境でのParquet / DuckDBによるデータ処理

---

# Architecture

## Lakehouse Architecture

Databricks上では、Bronze / Silver / Goldの3層構成を採用しています。

```text
                     GBIF Occurrence API
                              │
                              ▼
                  ┌────────────────────┐
                  │ Data Ingestion     │
                  │ Python / Requests  │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Bronze             │
                  │ Raw JSON           │
                  │ Delta Lake         │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Silver             │
                  │ PySpark            │
                  │ Structured Data    │
                  │ Data Quality       │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │ Gold               │
                  │ Analytical Tables  │
                  └────────────────────┘
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

Job Parametersとして以下を指定できます。

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

### API Retry

GBIF APIへのアクセスでは、一時的なサーバーエラーを考慮したRetry処理を実装しています。

Retry対象として以下のHTTPステータスコードを扱います。

```text
502 Bad Gateway
503 Service Unavailable
504 Gateway Timeout
```

Retry時にはExponential Backoffを利用し、一時的なAPI障害によるパイプライン失敗を抑制しています。

取得したAPIレスポンスはRaw JSONとしてBronze Delta Tableへ保存します。

```text
workspace.bronze.gbif_occurrences
```

Bronze LayerではRawデータを可能な限り保持し、後続処理で必要な項目を追加・再処理できる構成としています。

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
   ├── Required Fields
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

Databricks JobはGUIだけで構築するのではなく、**Databricks Asset Bundles**を利用してコードとして定義しています。

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

これらのパラメータをNotebook Taskへ渡すことで、GBIF APIから取得する対象種と取得件数を制御しています。

---

## Bundle Commands

Databricks CLIを使用して、Gitで管理されたJob定義をDatabricksへデプロイできます。

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run gbif_pipeline -t dev
```

これにより、Databricks Jobの構成をコードとして管理・再現できます。

---

# CI/CD

GitHub Actionsを利用して、PythonコードとDatabricks Bundleの自動検証・デプロイを実装しています。

## Pull Request

Pull Requestでは以下の検証を実行します。

```text
Pull Request
     │
     ├── Ruff
     ├── pytest
     └── databricks bundle validate
```

コード品質、ユニットテスト、Databricks Job定義を検証し、問題がある場合はデプロイを行いません。

## mainへのPush

`main`ブランチへのPushでは、検証に加えてDatabricksへのBundleデプロイを実行します。

```text
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

これにより、GitHubをソースコードのSingle Source of Truthとして、Databricks Jobの構成を自動的に反映できる構成としています。

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

# Local Development

Databricks版とは別に、ローカル環境でもGBIF APIからデータを取得し、Parquet・DuckDBによるデータ処理・分析を実行できます。

ローカル環境では開発・テストを行い、Databricks環境ではPySparkとDelta Lakeを利用して実行する構成としています。

## Local Architecture

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

## Setup

### 1. Clone

```bash
git clone https://github.com/hk0753854/gbif-database.git
cd gbif-database
```

### 2. Create Virtual Environment

Windows:

```bash
python -m venv .venv
```

### 3. Activate

PowerShell:

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

# Testing & Code Quality

pytestによるユニットテストを実装しています。

現在の主なテスト対象：

```text
GBIF API Client
API Pagination
DataFrame Transformation
Data Validation
Pipeline
DuckDB Query
CLI
```

APIアクセス部分ではpytestの`monkeypatch`を使用し、実際のGBIF APIへアクセスせずにテストできる構成としています。

### Run Tests

```bash
pytest
```

### Run Ruff

```bash
ruff check src tests
```

これらのチェックはGitHub Actionsでも自動実行されます。

---

# Project Structure

```text
gbif-database/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── notebooks/
│   └── databricks/
│       ├── 01_ingest_gbif_to_bronze.py
│       ├── 02_transform_bronze_to_silver.py
│       └── 03_create_gold_tables.py
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

# Design Principles

このプロジェクトでは、単にDatabricks上でNotebookを実行するだけではなく、実際のデータエンジニアリング開発を想定して以下を意識しています。

### 1. Local Development

Pythonによるデータ処理をローカル環境で開発・テストします。

### 2. Automated Testing

pytestによるユニットテストを実装し、APIクライアントやデータ処理ロジックを自動検証します。

### 3. Code Quality

RuffによるLintをCI/CDへ組み込み、コード品質を自動チェックします。

### 4. Lakehouse Architecture

Databricks上ではBronze / Silver / Goldにデータを分離します。

### 5. Job as Code

Databricks JobをAsset Bundlesで定義し、Gitによるバージョン管理を行います。

### 6. CI/CD

GitHub ActionsからRuff、pytest、Bundle Validation、Bundle Deploymentを自動実行します。

```text
Local Development
       │
       ▼
      Git
       │
       ▼
    GitHub
       │
       ▼
GitHub Actions
       │
       ├── Ruff
       ├── pytest
       ├── Bundle Validate
       │
       ▼
 Bundle Deploy
       │
       ▼
 Databricks
       │
       ▼
Data Pipeline
```

---

# Future Improvements

今後は以下の機能追加を予定しています。

* 大規模データ取得に対応したPaginationの強化
* 増分取得
* 重複データの検出・Deduplication
* データ品質チェックの強化
* Databricks SQL Dashboard
* パイプライン実行結果の監視・ログ管理
* より高度な地理空間分析
* dev / prod環境の運用分離
* データパイプラインの実行履歴・メタデータ管理

API Retry、ユニットテスト、Ruff、Databricks Asset Bundles、GitHub ActionsによるCI/CDについては実装済みです。

---

# Portfolio Highlights

このプロジェクトでは、以下のデータエンジニアリング要素を一通り実装しています。

| Area                   | Implementation                     |
| ---------------------- | ---------------------------------- |
| Data Source            | GBIF Occurrence API                |
| Ingestion              | Python / Requests                  |
| API Reliability        | Retry / Exponential Backoff        |
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
| Local Storage          | Parquet                            |

---

# Learning Objectives

このプロジェクトを通して、以下のデータエンジニアリング技術を実践しています。

```text
REST APIからのデータ取得
        │
        ▼
ETL / ELT Pipeline
        │
        ├── Python
        ├── Pandas
        └── PySpark
        │
        ▼
Bronze / Silver / Gold Architecture
        │
        ▼
Delta Lake
        │
        ▼
Databricks Jobs
        │
        ▼
Databricks Asset Bundles
        │
        ▼
Git / GitHub
        │
        ▼
GitHub Actions CI/CD
```

---

# License

This project is for educational and portfolio purposes.
