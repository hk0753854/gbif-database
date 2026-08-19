# Databricks notebook source
import duckdb
import pandas as pd

PARQUET_PATH = "../data/occurrences.parquet"

df = pd.read_parquet(PARQUET_PATH)

print(f"Records: {len(df):,}")
print(f"Columns: {len(df.columns)}")

df.head()

# COMMAND ----------

df.info()

# COMMAND ----------

df.isna().sum().sort_values(ascending=False).head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データ品質
# MAGIC
# MAGIC 取得した663件のGBIF occurrenceデータについて、各カラムの欠損状況を確認する。
# MAGIC
# MAGIC 特に位置情報（緯度・経度）と観察日（eventDate）には欠損が存在するため、
# MAGIC 後続の地理分析・時系列分析では欠損データを考慮する必要がある。

# COMMAND ----------

missing = (
    df.isna()
    .sum()
    .sort_values(ascending=False)
)

missing = missing[missing > 0]

missing

# COMMAND ----------

missing_rate = (
    df.isna()
    .mean()
    .mul(100)
    .sort_values(ascending=False)
)

missing_rate = missing_rate[missing_rate > 0]

missing_rate

# COMMAND ----------

import matplotlib.pyplot as plt

missing_rate.sort_values().plot(
    kind="barh",
    figsize=(8, 4),
    title="Missing Value Rate",
)

plt.xlabel("Missing Rate (%)")
plt.ylabel("Column")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Species別の観察記録
# MAGIC
# MAGIC GBIF APIから取得したレコードが、実際にどの分類群に紐づいているかを確認する。

# COMMAND ----------

species_counts = (
    df.groupby("scientificName")
    .size()
    .sort_values(ascending=False)
)

species_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ### 考察
# MAGIC
# MAGIC `Hynobius nebulosus` を検索条件としてGBIF occurrence APIを利用したが、
# MAGIC 取得結果には検索対象とは異なる `scientificName` も含まれていた。
# MAGIC
# MAGIC そのため、GBIF APIから取得したデータをそのまま分析対象とするのではなく、
# MAGIC 分類情報を確認した上で、分析対象となるレコードを定義する必要がある。

# COMMAND ----------

species_counts.plot(
    kind="bar",
    figsize=(10, 5),
    title="Observations by Scientific Name",
)

plt.xlabel("Scientific Name")
plt.ylabel("Observations")
plt.xticks(rotation=45, ha="right")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Country別の観察記録
# MAGIC
# MAGIC GBIF occurrenceデータを国別に集計し、観察記録がどの地域に集中しているかを確認する。
# MAGIC
# MAGIC `countryCode` が欠損しているレコードについては、`UNKNOWN` として扱う。

# COMMAND ----------

country_counts = (
    df.assign(
        countryCode=df["countryCode"].fillna("UNKNOWN")
    )
    .groupby("countryCode")
    .size()
    .sort_values(ascending=False)
)

country_counts

# COMMAND ----------

country_counts.plot(
    kind="bar",
    figsize=(8, 5),
    title="Observations by Country",
)

plt.xlabel("Country Code")
plt.ylabel("Observations")
plt.xticks(rotation=0)
plt.show()

# COMMAND ----------

country_rate = (
    country_counts
    .div(country_counts.sum())
    .mul(100)
    .round(1)
)

country_rate

# COMMAND ----------

# MAGIC %md
# MAGIC ### 考察
# MAGIC
# MAGIC 観察記録の大部分は日本（JP）に集中している。
# MAGIC
# MAGIC 一方、39件のレコードでは `countryCode` が欠損している。
# MAGIC そのため、国別分析では欠損値を除外するのではなく、`UNKNOWN` として
# MAGIC 明示的に扱うことで、データ欠損の存在を維持した。
# MAGIC
# MAGIC また、`countryCode` が `JP` 以外のレコードも存在するため、
# MAGIC 検索対象の分類群と地理情報の組み合わせについても確認する必要がある。

# COMMAND ----------

unknown_country = (
    df[df["countryCode"].isna()]
    .groupby("scientificName")
    .size()
    .sort_values(ascending=False)
)

unknown_country

# COMMAND ----------

# MAGIC %md
# MAGIC ### Country情報が欠損しているレコード
# MAGIC
# MAGIC `countryCode` が欠損している39件について分類群別に確認したところ、
# MAGIC 37件が `Hynobius nebulosus`、2件が `BOLD:AAI8551` だった。
# MAGIC
# MAGIC このことから、国情報の欠損は特定の分類群に偏っていることが分かった。
# MAGIC
# MAGIC ただし、`countryCode` の欠損だけからデータ自体が不正であるとは判断できない。
# MAGIC GBIFでは位置情報が保護目的などで非公開・粗い精度に変更される場合もあるため、
# MAGIC 分析では欠損の理由を考慮する必要がある。

# COMMAND ----------

year_counts = (
    df["eventDate"]
    .dropna()
    .astype(str)
    .str[:4]
    .astype(int)
    .value_counts()
    .sort_index()
)

year_counts

# COMMAND ----------

year_counts.plot(
    figsize=(12, 5),
    title="Observations by Year",
)

plt.xlabel("Year")
plt.ylabel("Observations")
plt.show()

# COMMAND ----------

df_1910 = df[
    df["eventDate"]
    .fillna("")
    .astype(str)
    .str.startswith("1910")
]

df_1910["basisOfRecord"].value_counts()

# COMMAND ----------

df_1910["scientificName"].value_counts()

# COMMAND ----------

df_1910[
    [
        "scientificName",
        "eventDate",
        "basisOfRecord",
        "countryCode",
    ]
].head(20)

# COMMAND ----------

## 5.2 1910年の異常値を調査

年別集計では1910年に333件のレコードが集中していた。

この件数が実際の観察活動を示しているのかを確認するため、
1910年のレコードについて `basisOfRecord` と `scientificName` を調査した。

# COMMAND ----------

df_1910["basisOfRecord"].value_counts()

# COMMAND ----------

df_1910["scientificName"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 考察
# MAGIC
# MAGIC 1910年の333件はすべて `PRESERVED_SPECIMEN` であり、
# MAGIC 保存標本に由来するレコードだった。
# MAGIC
# MAGIC また、分類群を見ると `Hynobius nebulosus nebulosus` が184件、
# MAGIC `Hynobius peropus` が146件を占めていた。
# MAGIC
# MAGIC したがって、1910年の件数の急増は、1910年に観察活動が集中したことを
# MAGIC 直接意味するものではない。
# MAGIC
# MAGIC この結果から、GBIFの時系列データを分析する際には、
# MAGIC 単純な年別件数だけでなく `basisOfRecord` などのデータ属性を確認し、
# MAGIC レコードがどのように生成されたかを考慮する必要がある。

# COMMAND ----------

basis_counts = (
    df["basisOfRecord"]
    .value_counts()
)

basis_counts

# COMMAND ----------

basis_counts.plot(
    kind="bar",
    figsize=(8, 5),
    title="Observations by Basis of Record",
)

plt.xlabel("Basis of Record")
plt.ylabel("Records")
plt.xticks(rotation=45, ha="right")
plt.show()