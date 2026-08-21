# GBIF Data Pipeline

GBIF（Global Biodiversity Information Facility）の生物多様性データを題材にした、**Databricksベースのデータエンジニアリングポートフォリオ**です。

GBIF Occurrence APIから生物の観察データを取得し、Databricks上で **Bronze / Silver / Gold Architecture** に基づくデータパイプラインを構築しています。

また、ローカル開発環境では **Python / Pandas / Parquet / DuckDB** を利用し、Databricks環境では **PySpark / Delta Lake / Databricks Jobs** を利用しています。

Databricks Jobは **Databricks Asset Bundles** によるJob as CodeとしてGit管理し、GitHub ActionsによるCI/CDまで実装しています。

---

## Overview

このプロジェクトでは、`species_master.csv`で管理した対象種を起点としてGBIF Occurrence APIから観察データを取得し、Databricks Lakehouse上で段階的に処理します。

```text
Species Master
      │
      ▼
04_read_species_master
      │
      ▼
workspace.mstr.species_master
      │
      ▼
GBIF Occurrence API
      │
      ▼
01_ingest_gbif_to_bronze
      │
      ▼
Bronze
      │
      ▼
02_transform_bronze_to_silver
      │
      ▼
Silver
      │
      ▼
03_create_gold_tables
      │
      ▼
Gold
      │
      ▼
Databricks SQL Dashboard
```

### 主な実装内容

* GBIF Occurrence APIからのデータ取得
* Species Masterによる対象種管理
* Bronze / Silver / Gold Architecture
* Raw JSONのDelta Lake保存
* GBIF IDを利用した重複排除
* APIエラーに対するRetry処理
* PySparkによるデータ変換
* Data Qualityチェック
* Gold Layerでの分析用テーブル作成
* Geographic Gridによる観察地点集計
* Databricks Jobsによるオーケストレーション
* Databricks Asset BundlesによるJob as Code
* Job Parameterによる取得件数の制御
* GitHub ActionsによるCI/CD
* pytestによるユニットテスト
* Ruffによるコード品質チェック
* Databricks SQL Dashboardによる可視化
* Local環境でのPython / Pandas / Parquet / DuckDBによる開発・分析

---

# Architecture

## Overall Architecture

```text
                         GitHub
                            │
                            ▼
                    GitHub Actions
                    ┌───────┴───────┐
                    │               │
                  Ruff            pytest
                    │               │
                    └───────┬───────┘
                            │
                     Bundle Validate
                            │
                            ▼
                   Databricks Asset
                       Bundles
                            │
                            ▼
                       Databricks
                            │
                            ▼
                    Databricks Job
                            │
                            ▼
                     Species Master
                            │
                            ▼
                   GBIF Occurrence API
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
                     SQL Dashboard
```

---

# Databricks Lakehouse Architecture

Databricks上では、**Bronze / Silver / Gold** の3層構成を採用しています。

```text
                GBIF Occurrence API
                         │
                         ▼
              ┌────────────────────┐
              │      Ingestion     │
              │  Python / Requests │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │      Bronze        │
              │      Raw JSON      │
              │     Delta Lake     │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │      Silver        │
              │      PySpark       │
              │ Structured Data    │
              │   Data Quality     │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │       Gold         │
              │ Analytical Tables  │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │     Dashboard      │
              │   Databricks SQL   │
              └────────────────────┘
```

### Bronze

GBIF APIから取得したレスポンスをRaw JSONとして保持します。

主な目的：

* APIレスポンスの原形を保持
* 後続処理で必要な項目を柔軟に利用
* 再変換可能なRaw Layerとして利用

Table:

```text
workspace.bronze.gbif_occurrences
```

### Silver

BronzeのRaw JSONをPySparkで構造化し、分析に利用しやすい形式へ変換します。

また、基本的なData Qualityチェックを実施します。

### Gold

Silverから分析用途の集計テーブルを作成します。

---

# Species Master

対象種は以下のCSVで管理しています。

```text
src/input_files/species_master.csv
```

処理の流れ：

```text
src/input_files/species_master.csv
             │
             ▼
04_read_species_master.py
             │
             ▼
workspace.mstr.species_master
```

Species MasterをDatabricks上のMaster Tableとして管理し、そこから`scientific_name`を取得してGBIF Occurrence APIの検索に利用します。

これにより、API取得対象のSpeciesをNotebookへ直接ハードコードするのではなく、**Master Dataを起点としたパイプライン構成**としています。

Table:

```text
workspace.mstr.species_master
```

---

# Databricks Pipeline

Databricksでは4つのNotebookを依存関係付きのJobとして実行しています。

```text
04_read_species_master
          │
          ▼
01_ingest_gbif_to_bronze
          │
          ▼
02_transform_bronze_to_silver
          │
          ▼
03_create_gold_tables
```

---

## 01. Read Species Master

`src/input_files/species_master.csv`を読み込み、Databricks上にMaster Tableを作成します。

```text
src/input_files/species_master.csv
              │
              ▼
workspace.mstr.species_master
```

---

## 02. Ingest GBIF to Bronze

Species Masterから対象となる`scientific_name`を取得し、GBIF Occurrence APIから観察データを取得します。

取得件数はDatabricks Job Parameterの`limit`で制御します。

例：

```text
limit = 10
```

取得したAPIレスポンスはRaw JSONとしてBronze Delta Tableへ保存します。

```text
workspace.bronze.gbif_occurrences
```

### API Retry

GBIF APIへのアクセスでは、一時的な通信エラーに対応するためRetry処理を実装しています。

Retry対象：

* HTTP 502
* HTTP 503
* HTTP 504
* Timeout

Retry回数には上限を設け、指数バックオフを利用しています。

### 重複排除

Bronzeへの保存時にはGBIFの`gbifID`を利用して既存データとの重複を防止します。

Delta Lakeの`MERGE`を利用し、同一GBIF IDの重複登録を避ける構成としています。

---

# Bronze → Silver

Bronzeに保存したRaw JSONをPySparkで構造化します。

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

`eventDate`については、分析しやすいように以下へ分離しています。

```text
event_year
event_month
event_day
```

Silver Table：

```text
workspace.silver.gbif_occurrences
```

---

# Data Quality

Silver Layerへの変換時に、基本的なData Qualityチェックを実施しています。

現在チェックしている主な項目：

* `gbifID` の欠損
* `scientificName` の欠損
* `countryCode` の欠損
* 緯度の範囲チェック（-90 ～ 90）
* 経度の範囲チェック（-180 ～ 180）

処理イメージ：

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

現在のData Quality処理は、**品質メトリクスを算出・確認するためのチェック**として実装しています。

品質チェックに該当したレコードを自動的に除外したり、品質エラーを検知してPipeline全体を失敗させる仕組みではありません。

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

---

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

# Dashboard

Gold Layerで作成した分析用テーブルを利用して、Databricks SQL Dashboardを構築しています。

Dashboardでは以下のような観点からデータを分析できます。

* Species別観察件数
* Country別観察件数
* 年別観察件数
* Observation Type
* Geographic Distribution

Dashboard定義はGitで管理しています。

```text
GBIF Biodiversity Dashboard.lvdash.json
```

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

Jobの依存関係：

```text
gbif_pipeline
│
├── read_species_master
│
├── ingest_gbif_to_bronze
│     └── depends_on:
│           read_species_master
│
├── transform_bronze_to_silver
│     └── depends_on:
│           ingest_gbif_to_bronze
│
└── create_gold_tables
      └── depends_on:
            transform_bronze_to_silver
```

これにより、Jobの構成・Notebook・依存関係をGitで管理し、再現可能な形でDatabricksへデプロイできます。

---

# Job Parameters

現在のDatabricks Jobで利用しているJob Parameterは以下です。

```text
limit
```

例：

```text
limit = 10
```

`limit`を利用することで、Notebookコードを変更せずにGBIF APIから取得する観察データ件数を変更できます。

Speciesの検索対象はJob Parameterではなく、**Species Masterの`scientific_name`から取得**します。

---

# Bundle Commands

Databricks CLIを利用して、Gitで管理されたJob定義をDatabricksへデプロイできます。

### Validate

```bash
databricks bundle validate -t dev
```

### Deploy

```bash
databricks bundle deploy -t dev
```

### Run

```bash
databricks bundle run gbif_pipeline -t dev
```

これにより、Databricks Jobをコードとして再現可能な状態で管理しています。

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

これにより、

* Pythonコードの品質
* Unit Test
* Databricks Job定義

を自動検証します。

---

## mainへのPush

`main`ブランチへのPushでは、検証に加えてDatabricksへのBundle Deploymentを実行します。

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

GitHub ActionsからDatabricksへ接続するための認証情報はGitHub Actions Secretsで管理しています。

```text
DATABRICKS_HOST
DATABRICKS_TOKEN
```

認証情報そのものをGitリポジトリへ保存しない構成としています。

---

## Production Deployment

Production環境へのDeployment用Workflowも用意しています。

```text
.github/workflows/deploy-prod.yml
```

Production Deploymentは通常の`main` Pushとは分離し、Production向けのBundle ValidateおよびDeployを実行する構成としています。

---

# Local Development

Databricks版とは別に、ローカル環境でもGBIF APIからデータを取得し、Parquet・DuckDBによるデータ処理・分析を実行できます。

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

Local環境ではPythonによる開発・テストを行い、Databricks版ではPySpark / Delta Lake / Databricks Jobsを利用する構成としています。

---

# Local Configuration

Local版では`config.yaml`で基本的な実行設定を管理しています。

```yaml
scientific_name: Hynobius nebulosus
limit: 100
output_path: data/occurrences.parquet
```

Databricks版ではJob Parameterを利用し、Local版では`config.yaml`を利用することで、それぞれの実行環境に適した設定方法を採用しています。

---

# Local Setup

## 1. Clone

```bash
git clone https://github.com/hk0753854/gbif-database.git
cd gbif-database
```

## 2. Create Virtual Environment

Windows:

```bash
python -m venv .venv
```

## 3. Activate

PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

## 4. Install Dependencies

```bash
pip install -e ".[dev]"
```

## 5. Run

```bash
python -m gbif_data_pipeline.cli "Hynobius nebulosus" --limit 10
```

---

# Local Processing

Local版では以下の責務分離を行っています。

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

### `gbif_client.py`

GBIF APIへのアクセスを担当します。

### `transform.py`

APIレスポンスをDataFrameへ変換します。

### `validate.py`

取得データに対する基本的な品質チェックを担当します。

### `pipeline.py`

データ取得・変換・検証処理をオーケストレーションします。

### `duckdb_client.py`

Parquetデータに対するSQL分析を担当します。

### `config.py`

Local実行時の設定ファイルを読み込みます。

### `logging_config.py`

アプリケーションのLogging設定を管理します。

---

# Testing

pytestによるユニットテストを実装しています。

現在の主なテスト対象：

```text
GBIF API Client
API Pagination
Configuration
DataFrame Transformation
Data Validation
Pipeline
DuckDB Query
CLI
Logging
```

APIアクセス部分ではpytestの`monkeypatch`を利用し、実際のGBIF APIへアクセスせずにテストできる構成としています。

テスト実行：

```bash
pytest
```

---

# Code Quality

RuffによるLintを利用しています。

```bash
ruff check src tests
```

コード品質チェックはGitHub Actionsでも自動実行されます。

---

# Technology Stack

| Category               | Technology               |
| ---------------------- | ------------------------ |
| Language               | Python 3.12              |
| API                    | GBIF Occurrence API      |
| Data Processing        | PySpark / Pandas         |
| Data Platform          | Databricks               |
| Storage                | Delta Lake / Parquet     |
| Local SQL Engine       | DuckDB                   |
| Architecture           | Bronze / Silver / Gold   |
| Orchestration          | Databricks Jobs          |
| Job Deployment         | Databricks Asset Bundles |
| Infrastructure as Code | Databricks Asset Bundles |
| Testing                | pytest                   |
| Linting                | Ruff                     |
| Version Control        | Git / GitHub             |
| CI/CD                  | GitHub Actions           |
| Visualization          | Databricks SQL Dashboard |

---

# Project Structure

```text
gbif-database/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy-prod.yml
│
├── notebooks/
│   └── databricks/
│       ├── 01_ingest_gbif_to_bronze.py
│       ├── 02_transform_bronze_to_silver.py
│       ├── 03_create_gold_tables.py
│       └── 04_read_species_master.py
│
├── resources/
│   └── gbif_pipeline.yml
│
├── src/
│   ├── gbif_data_pipeline/
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   ├── cli.py
│   │   ├── config.py
│   │   ├── duckdb_client.py
│   │   ├── gbif_client.py
│   │   ├── logging_config.py
│   │   ├── pipeline.py
│   │   ├── transform.py
│   │   └── validate.py
│   │
│   └── input_files/
│       └── species_master.csv
│
├── tests/
│   ├── test_cli.py
│   ├── test_config.py
│   ├── test_duckdb_client.py
│   ├── test_gbif_client.py
│   ├── test_pipeline.py
│   ├── test_transform.py
│   └── test_validate.py
│
├── .gitignore
├── config.yaml
├── databricks.yml
├── GBIF Biodiversity Dashboard.lvdash.json
├── pyproject.toml
└── README.md
```

---

# Design Principles

このプロジェクトでは、単にDatabricks Notebookを実行するだけではなく、実際のデータエンジニアリング開発を想定して以下を意識しています。

## 1. Separation of Responsibilities

データ取得・変換・品質チェック・分析処理の責務を分離しています。

```text
API Client
    │
    ▼
Transformation
    │
    ▼
Validation
    │
    ▼
Pipeline
    │
    ▼
Analytics
```

---

## 2. Master Data Driven Pipeline

Species Masterをパイプラインの起点としています。

```text
Species Master
      │
      ▼
scientific_name
      │
      ▼
GBIF API
      │
      ▼
Occurrence Data
```

これにより、API取得対象をNotebookへ直接ハードコードするのではなく、Master Dataによって管理できる構成としています。

---

## 3. Local Development

Pythonによるデータ処理をローカル環境で開発・テストできる構成としています。

これにより、Databricks環境に依存せずに基本的な処理ロジックを検証できます。

---

## 4. Lakehouse Architecture

Databricks上ではBronze / Silver / Goldにデータを分離しています。

```text
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
```

Rawデータ、構造化データ、分析用データを分離することで、各Layerの責務を明確にしています。

---

## 5. Data Quality

Silver Layerへの変換時に基本的なデータ品質チェックを実施しています。

必須項目や緯度・経度の範囲などを確認し、分析用データを作成する前に品質を確認できる構成としています。

---

## 6. Job as Code

Databricks JobをDatabricks Asset Bundlesとしてコード管理しています。

```text
Git
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

Notebook・Job・依存関係をGitで管理することで、Job構成を再現可能にしています。

---

## 7. Automated CI/CD

GitHub Actionsによって、Pull Requestおよび`main`ブランチへのPush時に自動検証を実行しています。

```text
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
```

---

# Portfolio Highlights

このプロジェクトでは、以下のデータエンジニアリング要素を実装しています。

| Area                   | Implementation                     |
| ---------------------- | ---------------------------------- |
| Data Source            | GBIF Occurrence API                |
| Data Ingestion         | Python / Requests                  |
| Master Data            | Species Master                     |
| API Reliability        | Retry / Timeout Handling           |
| Deduplication          | Delta Lake MERGE                   |
| Local Processing       | Pandas                             |
| Distributed Processing | PySpark                            |
| Data Lakehouse         | Databricks / Delta Lake            |
| Architecture           | Bronze / Silver / Gold             |
| Data Quality           | Required Fields / Range Validation |
| Analytics              | Gold Aggregation Tables            |
| Geospatial Processing  | Geographic Grid Aggregation        |
| Orchestration          | Databricks Jobs                    |
| Infrastructure as Code | Databricks Asset Bundles           |
| Job as Code            | Databricks Asset Bundles           |
| Parameterization       | Databricks Job Parameters          |
| Testing                | pytest                             |
| Code Quality           | Ruff                               |
| CI/CD                  | GitHub Actions                     |
| Version Control        | Git / GitHub                       |
| Local Storage          | Parquet                            |
| Local Analytics        | DuckDB                             |
| Visualization          | Databricks SQL Dashboard           |

---

# Future Improvements

今後は以下の機能追加を検討しています。

* Incremental ingestion
* より高度な重複検知・Idempotent processing
* Enhanced data quality monitoring
* dev / prod environment separation
* Pipeline execution monitoring
* Advanced geospatial analysis
* API rate-limit handlingの強化
* より高度なAPI pagination制御

---

# Learning Objectives

このプロジェクトを通して、以下のデータエンジニアリング技術を実践しています。

```text
REST API Data Ingestion
          │
          ▼
     ETL / ELT
          │
          ▼
 Python Data Processing
          │
          ▼
       PySpark
          │
          ▼
 Bronze / Silver / Gold
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
      Job as Code
          │
          ▼
     Data Quality
          │
          ▼
   Gold Analytics
          │
          ▼
Databricks SQL Dashboard
```

ソフトウェア開発の観点では、

```text
Git
 │
 ▼
GitHub
 │
 ▼
Pull Request
 │
 ▼
GitHub Actions
 ├── Ruff
 ├── pytest
 └── Bundle Validate
 │
 ▼
Databricks Bundle Deploy
 │
 ▼
Databricks Job
```

という開発・テスト・デプロイフローを実装しています。

---

# Conclusion

このプロジェクトでは、GBIFの生物多様性データを題材として、

```text
APIからのデータ取得
        ↓
Master Dataによる対象種管理
        ↓
Bronze LayerへのRaw Data保存
        ↓
Silver Layerへの構造化
        ↓
Data Quality Check
        ↓
Gold Layerでの分析用データ作成
        ↓
Dashboardによる可視化
```

までの一連のデータパイプラインを構築しています。

さらに、

```text
Local Development
        ↓
Git
        ↓
Automated Testing
        ↓
CI/CD
        ↓
Databricks Deployment
```

というソフトウェア開発のワークフローも取り入れています。

単純なNotebookによるデータ分析ではなく、**再現性・テスト・コード管理・CI/CD・Job as Code・Lakehouse Architecture**を意識したデータエンジニアリングプロジェクトとして構築しています。

---

# License

This project is for educational and portfolio purposes.
