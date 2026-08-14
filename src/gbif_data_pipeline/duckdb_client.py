import duckdb
import pandas as pd


def query_parquet(
    parquet_path: str,
    sql: str,
) -> pd.DataFrame:
    """Parquetに対してSQLを実行する。"""

    connection = duckdb.connect()

    try:
        return connection.execute(
            sql,
            [str(parquet_path)],
        ).df()
    finally:
        connection.close()


def count_by_country(
    parquet_path: str,
) -> pd.DataFrame:
    """国ごとの観察数を集計する。"""

    sql = """
        SELECT
            countryCode,
            COUNT(*) AS observations
        FROM read_parquet(?)
        GROUP BY countryCode
        ORDER BY observations DESC
    """

    return query_parquet(
        parquet_path,
        sql,
    )


def count_by_year(
    parquet_path: str,
) -> pd.DataFrame:
    """年ごとの観察数を集計する。"""

    sql = """
        SELECT
            TRY_CAST(
                LEFT(eventDate, 4) AS INTEGER
            ) AS year,
            COUNT(*) AS observations
        FROM read_parquet(?)
        WHERE eventDate IS NOT NULL
        GROUP BY year
        ORDER BY year
    """

    return query_parquet(
        parquet_path,
        sql,
    )


def count_by_species(
    parquet_path: str,
) -> pd.DataFrame:
    """種ごとの観察数を集計する。"""

    sql = """
        SELECT
            scientificName,
            COUNT(*) AS observations
        FROM read_parquet(?)
        WHERE scientificName IS NOT NULL
        GROUP BY scientificName
        ORDER BY observations DESC
    """

    return query_parquet(
        parquet_path,
        sql,
    )