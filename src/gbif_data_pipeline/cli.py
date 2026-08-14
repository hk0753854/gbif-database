import argparse

from gbif_data_pipeline.duckdb_client import (
    count_by_country,
    count_by_species,
    count_by_year,
)
from gbif_data_pipeline.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="GBIF occurrence data pipeline",
    )

    parser.add_argument(
        "--scientific-name",
        required=True,
        help="Scientific name to search",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of occurrences to retrieve",
    )

    parser.add_argument(
        "--output",
        default="data/occurrences.parquet",
        help="Output Parquet path",
    )

    args = parser.parse_args()

    output_path = run_pipeline(
        scientific_name=args.scientific_name,
        limit=args.limit,
        output_path=args.output,
    )

    print(f"Pipeline completed: {output_path}")

    print("\n=== Observations by Species ===")
    print(count_by_species(output_path))

    print("\n=== Observations by Country ===")
    print(count_by_country(output_path))

    print("\n=== Observations by Year ===")
    print(count_by_year(output_path))


if __name__ == "__main__":
    main()