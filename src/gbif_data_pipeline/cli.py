import argparse

from gbif_data_pipeline.config import load_config
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
        help="Scientific name to search",
        )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of occurrences to retrieve",
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to YAML configuration file",
    )

    args = parser.parse_args()

    config = {}

    if args.config:
        config = load_config(args.config)

    scientific_name = (
        args.scientific_name
        or config.get("scientific_name")
    )

    limit = (
        args.limit
        if args.limit is not None
        else config.get("limit", 100)
    )

    output_path = config.get(
        "output_path",
        "data/occurrences.parquet",
    )

    if not scientific_name:
        parser.error(
            "scientific_name or --config with scientific_name is required"
        )

    output_path = run_pipeline(
        scientific_name=scientific_name,
        limit=limit,
        output_path=output_path,
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